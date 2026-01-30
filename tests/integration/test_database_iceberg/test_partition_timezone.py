import glob
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta

import pyarrow as pa
import pytest
import requests
import urllib3
import pytz
from minio import Minio
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    DoubleType,
    LongType,
    FloatType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType
)
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER

from helpers.cluster import ClickHouseCluster, ClickHouseInstance, is_arm
from helpers.config_cluster import minio_secret_key, minio_access_key
from helpers.s3_tools import get_file_contents, list_s3_objects, prepare_s3_bucket
from helpers.test_tools import TSV, csv_compare
from helpers.config_cluster import minio_secret_key

BASE_URL = "http://rest:8181/v1"
BASE_URL_LOCAL = "http://localhost:8182/v1"
BASE_URL_LOCAL_RAW = "http://localhost:8182"

CATALOG_NAME = "demo"

DEFAULT_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
    )
)
DEFAULT_SORT_ORDER = SortOrder(SortField(source_id=2, transform=IdentityTransform()))
DEFAULT_SCHEMA = Schema(
    NestedField(field_id=1, name="datetime", field_type=TimestampType(), required=False),
    NestedField(field_id=2, name="value", field_type=LongType(), required=False),
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=["configs/timezone.xml", "configs/cluster.xml"],
            user_configs=["configs/iceberg_partition_timezone.xml"],
            stay_alive=True,
            with_iceberg_catalog=True,
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        # TODO: properly wait for container
        time.sleep(10)

        yield cluster

    finally:
        cluster.shutdown()


def load_catalog_impl(started_cluster):
    return load_catalog(
        CATALOG_NAME,
        **{
            "uri": BASE_URL_LOCAL_RAW,
            "type": "rest",
            "s3.endpoint": f"http://{started_cluster.get_instance_ip('minio')}:9000",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )


def create_table(
    catalog,
    namespace,
    table,
    schema=DEFAULT_SCHEMA,
    partition_spec=DEFAULT_PARTITION_SPEC,
    sort_order=DEFAULT_SORT_ORDER,
):
    return catalog.create_table(
        identifier=f"{namespace}.{table}",
        schema=schema,
        location=f"s3://warehouse-rest/data",
        partition_spec=partition_spec,
        sort_order=sort_order,
    )


def create_clickhouse_iceberg_database(
    node, name, additional_settings={}, engine='DataLakeCatalog'
):
    settings = {
        "catalog_type": "rest",
        "warehouse": "demo",
        "storage_endpoint": "http://minio:9000/warehouse-rest",
    }

    settings.update(additional_settings)

    node.query(
        f"""
DROP DATABASE IF EXISTS {name};
SET allow_database_iceberg=true;
SET write_full_path_in_iceberg_metadata=1;
CREATE DATABASE {name} ENGINE = {engine}('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
    """
    )
    show_result = node.query(f"SHOW DATABASE {name}")
    assert minio_secret_key not in show_result
    assert "HIDDEN" in show_result


def test_partition_timezone(started_cluster):
    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace("timezone_ns")
    table = create_table(
        catalog,
        "timezone_ns",
        "tz_table",
    )

    # catalog accept data in UTC
    data = [{"datetime": datetime(2024, 1, 1, 20, 0), "value": 1}, # partition 20240101
            {"datetime": datetime(2024, 1, 1, 23, 0), "value": 2}, # partition 20240101
            {"datetime": datetime(2024, 1, 2, 2, 0), "value": 3}]  # partition 20240102
    df = pa.Table.from_pylist(data)
    table.append(df)

    node = started_cluster.instances["node1"]
    create_clickhouse_iceberg_database(node, CATALOG_NAME)

    # server timezone is Asia/Istanbul (UTC+3)
    assert node.query(f"""
                      SELECT datetime, value
                      FROM {CATALOG_NAME}.`timezone_ns.tz_table`
                      ORDER BY datetime
                      """, timeout=10) == TSV(
        [
            ["2024-01-01 23:00:00.000000", 1],
            ["2024-01-02 02:00:00.000000", 2],
            ["2024-01-02 05:00:00.000000", 3],
        ])
    
    # partitioning works correctly
    assert node.query(f"""
                      SELECT datetime, value
                      FROM {CATALOG_NAME}.`timezone_ns.tz_table`
                      WHERE datetime >= '2024-01-02 00:00:00'
                      ORDER BY datetime
                      """, timeout=10) == TSV(
        [
            ["2024-01-02 02:00:00.000000", 2],
            ["2024-01-02 05:00:00.000000", 3],
        ])
