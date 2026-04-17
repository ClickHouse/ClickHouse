import logging
import time
import uuid
import os
from datetime import datetime

import pyarrow as pa
import pytest
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    LongType,
    NestedField,
    TimestampType,
)

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key, minio_access_key
from helpers.test_tools import TSV

CATALOG_NAME = "test"

BASE_URL = "http://glue:3000"

DEFAULT_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
    )
)
DEFAULT_SORT_ORDER = SortOrder(SortField(source_id=1, transform=DayTransform()))
DEFAULT_SCHEMA = Schema(
    NestedField(field_id=1, name="datetime", field_type=TimestampType(), required=False),
    NestedField(field_id=2, name="value", field_type=LongType(), required=False),
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        # We must add some credentials, otherwise moto (AWS Mock)
        # will reject boto connection
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            stay_alive=True,
            with_glue_catalog=True,
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        # TODO: properly wait for container
        time.sleep(10)

        yield cluster

    finally:
        cluster.shutdown()


def get_glue_local_url(cluster):
    return f"http://localhost:{cluster.glue_catalog_port}"


def load_catalog_impl(started_cluster):
    return load_catalog(
        CATALOG_NAME,  # name is not important
        **{
            "type": "glue",
            "glue.endpoint": get_glue_local_url(started_cluster),
            "glue.region": "us-east-1",
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
    dir="data"
):
    return catalog.create_table(
        identifier=f"{namespace}.{table}",
        schema=schema,
        location=f"s3://warehouse-glue/{dir}",
        partition_spec=partition_spec,
        sort_order=sort_order,
    )


def create_clickhouse_glue_database(
    node, name, additional_settings={}, with_credentials=True
):
    settings = {
        "catalog_type": "glue",
        "warehouse": "test",
        "storage_endpoint": "http://minio:9000/warehouse-glue",
        "region": "us-east-1",
    }

    settings.update(additional_settings)

    credential_args = f",'{minio_access_key}', '{minio_secret_key}'" if with_credentials else ""

    node.query(
        f"""
DROP DATABASE IF EXISTS {name};
CREATE DATABASE {name} ENGINE = DataLakeCatalog('{BASE_URL}'{credential_args})
SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
    """,
        settings={
            "allow_database_glue_catalog": 1,
            "write_full_path_in_iceberg_metadata": 1,
        },
    )


def test_partition_timezone(started_cluster):
    catalog = load_catalog_impl(started_cluster)
    namespace = f"timezone_ns_{uuid.uuid4()}"
    table_name = f"tz_table__{uuid.uuid4()}"
    catalog.create_namespace(namespace)
    table = create_table(
        catalog,
        namespace,
        table_name,
    )

    # catalog accept data in UTC
    data = [{"datetime": datetime(2024, 1, 1, 20, 0), "value": 1}, # partition 20240101
            {"datetime": datetime(2024, 1, 1, 23, 0), "value": 2}, # partition 20240101
            {"datetime": datetime(2024, 1, 2, 2, 0), "value": 3}]  # partition 20240102
    df = pa.Table.from_pylist(data)
    table.append(df)

    node = started_cluster.instances["node1"]
    create_clickhouse_glue_database(node, CATALOG_NAME)

    # server timezone is default UTC
    assert node.query(f"""
                      SELECT datetime, value
                      FROM {CATALOG_NAME}.`{namespace}.{table_name}`
                      ORDER BY datetime
                      """, timeout=10) == TSV(
        [
            ["2024-01-01 20:00:00.000000", 1],
            ["2024-01-01 23:00:00.000000", 2],
            ["2024-01-02 02:00:00.000000", 3],
        ])
    
    # partitioning works correctly
    assert node.query(f"""
                      SELECT datetime, value
                      FROM {CATALOG_NAME}.`{namespace}.{table_name}`
                      WHERE datetime >= '2024-01-02 00:00:00'
                      ORDER BY datetime
                      """, timeout=10) == TSV(
        [
            ["2024-01-02 02:00:00.000000", 3],
        ])

    assert node.query(f"""
                      SELECT datetime, value
                      FROM {CATALOG_NAME}.`{namespace}.{table_name}`
                      WHERE datetime < '2024-01-02 00:00:00'
                      ORDER BY datetime
                      """, timeout=10) == TSV(
        [
            ["2024-01-01 20:00:00.000000", 1],
            ["2024-01-01 23:00:00.000000", 2],
        ])
