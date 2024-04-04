# Copyright (c) 2024 Linh Pham
# wwdtm_import_location_latlong is released under the terms of the Apache License 2.0
# SPDX-License-Identifier: Apache-2.0
#
# vim: set noai syntax=python ts=4 sw=4:
"""Wait Wait Stats Location Latitude/Longitude Import Script."""
import csv
import json
import sys
from argparse import ArgumentParser, Namespace
from decimal import Decimal
from pathlib import Path

from mysql.connector import connect as mysql_connect
from mysql.connector.connection import MySQLConnection
from mysql.connector.pooling import PooledMySQLConnection


def parse_command() -> Namespace:
    """Parse command-line argument, options and flags."""
    parser: ArgumentParser = ArgumentParser(
        description="Populates NPR.org show URLs in the Wait Wait Stats Database."
    )
    parser.add_argument(
        "-f",
        "--file",
        help="CSV file containing location information, including latitude and longitude fields",
        type=str,
    )

    return parser.parse_args()


def parse_database_config(
    config_file: str = "config.json",
) -> dict[str, str | int | bool] | None:
    """Parse configuration JSON file."""
    config_file_path: Path = Path.cwd() / config_file
    with config_file_path.open(mode="r", encoding="utf-8") as config:
        connect_config = json.load(config)

    if not connect_config or "database" not in connect_config:
        return None

    database_config: dict[str, str | int | bool] = connect_config["database"]
    if "autocommit" not in database_config or not database_config["autocommit"]:
        database_config["autocommit"] = True

    return database_config


def read_csv(file_name: str) -> list[dict[str, int | Decimal]]:
    """Returns a dictionary containing the contents of the locations CSV file."""
    csv_file: Path = Path(file_name)
    locations: list = []
    with csv_file.open(mode="r", encoding="utf-8") as locations_file:
        reader = csv.DictReader(locations_file)

        for line in reader:
            location: dict[str, int | Decimal] = {}
            location["id"] = int(line["locationid"])
            location["latitude"] = (
                Decimal(line["latitude"]) if line["latitude"].strip() else None
            )
            location["longitude"] = (
                Decimal(line["longitude"]) if line["longitude"].strip() else None
            )
            locations.append(location)

    return locations


def update_location_lat_long(
    locations: list[dict[str, int | Decimal]],
    database_connection: MySQLConnection | PooledMySQLConnection,
) -> None:
    """Updates the location entries in the database with latitude and longitude values."""
    if not locations:
        print("ERROR: No location information provided.")
        return None

    for location in locations:
        if location["latitude"] and location["longitude"]:
            cursor = database_connection.cursor()
            query = """
                UPDATE ww_locations SET latitude = %s, longitude = %s
                WHERE locationid = %s;
            """
            cursor.execute(
                query,
                (
                    location["latitude"],
                    location["longitude"],
                    location["id"],
                ),
            )
            cursor.close()

    return None


def main() -> None:
    """Application entry."""
    command_args: Namespace = parse_command()
    database_config = parse_database_config(config_file="config.json")

    if not database_config:
        print("ERROR: Database configuration file is not valid.")
        sys.exit(1)

    locations = read_csv(file_name=command_args.file)
    if not locations:
        print("INFO: No locations found in CSV file. Exiting.")
        sys.exit(0)

    database_connection = mysql_connect(**database_config)
    update_location_lat_long(
        locations=locations, database_connection=database_connection
    )

    return None


if __name__ == "__main__":
    main()
