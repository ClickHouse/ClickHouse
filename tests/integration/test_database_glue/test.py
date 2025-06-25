import logging
import os
import random
import uuid
from datetime import datetime

import pyarrow as pa
import pytest
import urllib3
from datetime import datetime, timedelta
from minio import Minio
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import DayTransform, IdentityTransform
from helpers.config_cluster import minio_access_key, minio_secret_key
import decimal
from pyiceberg.types import (
    DoubleType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    MapType,
    DecimalType,
)

from helpers.cluster import ClickHouseCluster, ClickHouseInstance, is_arm

import boto3

CATALOG_NAME = "test"

BASE_URL = "http://glue:3000"
BASE_URL_LOCAL_HOST = "http://localhost:3000"

def generate_decimal(precision=9, scale=2):
    max_value = 10**(precision - scale) - 1
    value = random.uniform(0, max_value)
    return round(decimal.Decimal(value), scale)

DEFAULT_SCHEMA = Schema(
    NestedField(
        field_id=1, name="datetime", field_type=TimestampType(), required=False
    ),
    NestedField(field_id=2, name="symbol", field_type=StringType(), required=False),
    NestedField(field_id=3, name="bid", field_type=DoubleType(), required=False),
    NestedField(field_id=4, name="ask", field_type=DoubleType(), required=False),
    NestedField(
        field_id=5,
        name="details",
        field_type=StructType(
            NestedField(
                field_id=4,
                name="created_by",
                field_type=StringType(),
                required=False,
            ),
        ),
        required=False,
    ),
    NestedField(
        field_id=6,
        name="map_string_decimal",
        field_type=MapType(
            key_type=StringType(),
            value_type=DecimalType(9, 2),
            key_id=7,
            value_id=8,
            value_required=False,
        ),
        required=False,
    ),
)

DEFAULT_CREATE_TABLE = "CREATE TABLE {}.`{}.{}`\\n(\\n    `datetime` Nullable(DateTime64(6)),\\n    `symbol` Nullable(String),\\n    `bid` Nullable(Float64),\\n    `ask` Nullable(Float64),\\n    `details` Tuple(created_by Nullable(String)),\\n    `map_string_decimal` Map(String, Nullable(Decimal(9, 2)))\\n)\\nENGINE = Iceberg(\\'http://minio:9000/warehouse-glue/data/\\', \\'minio\\', \\'[HIDDEN]\\')\n"

DEFAULT_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
    )
)

DEFAULT_SORT_ORDER = SortOrder(SortField(source_id=2, transform=IdentityTransform()))


def list_databases():
    client = boto3.client(
        "glue", region_name="us-east-1", endpoint_url=BASE_URL_LOCAL_HOST
    )
    databases = client.get_databases()
    return databases


def load_catalog_impl(started_cluster):
    return load_catalog(
        CATALOG_NAME,  # name is not important
        **{
            "type": "glue",
            "glue.endpoint": BASE_URL_LOCAL_HOST,
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
):
    return catalog.create_table(
        identifier=f"{namespace}.{table}",
        schema=schema,
        location="s3://warehouse-glue/data",
        partition_spec=partition_spec,
        sort_order=sort_order,
    )



def generate_arrow_data(num_rows=5):
    datetimes = []
    symbols = []
    bids = []
    asks = []
    details_created_by = []
    map_keys = []
    map_values = []

    offsets = [0]

    for _ in range(num_rows):
        datetimes.append(datetime.utcnow() - timedelta(minutes=random.randint(0, 60)))
        symbols.append(random.choice(["AAPL", "GOOG", "MSFT"]))
        bids.append(random.uniform(100, 150))
        asks.append(random.uniform(150, 200))
        details_created_by.append(random.choice(["alice", "bob", "carol"]))

        # map<string, decimal(9,2)>
        keys = []
        values = []
        for i in range(random.randint(1, 3)):
            keys.append(f"key{i}")
            values.append(generate_decimal())
        map_keys.extend(keys)
        map_values.extend(values)
        offsets.append(offsets[-1] + len(keys))

    # Struct for 'details'
    struct_array = pa.StructArray.from_arrays(
        [pa.array(details_created_by, type=pa.string())],
        names=["created_by"]
    )

    # Map array
    map_array = pa.MapArray.from_arrays(
        offsets=pa.array(offsets, type=pa.int32()),
        keys=pa.array(map_keys, type=pa.string()),
        items=pa.array(map_values, type=pa.decimal128(9, 2))
    )

    # Final table
    table = pa.table({
        "datetime": pa.array(datetimes, type=pa.timestamp("us")),
        "symbol": pa.array(symbols, type=pa.string()),
        "bid": pa.array(bids, type=pa.float64()),
        "ask": pa.array(asks, type=pa.float64()),
        "details": struct_array,
        "map_string_decimal": map_array,
    })

    return table

def create_clickhouse_glue_database(
    started_cluster, node, name, additional_settings={}
):
    settings = {
        "catalog_type": "glue",
        "warehouse": "test",
        "storage_endpoint": "http://minio:9000/warehouse-glue",
        "region": "us-east-1",
    }

    settings.update(additional_settings)

    node.query(
        f"""
DROP DATABASE IF EXISTS {name};
SET allow_experimental_database_glue_catalog=true;
CREATE DATABASE {name} ENGINE = DataLakeCatalog('{BASE_URL}', '{minio_access_key}', '{minio_secret_key}')
SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
    """
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
            main_configs=[],
            user_configs=[],
            stay_alive=True,
            with_glue_catalog=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_list_tables(started_cluster):
    node = started_cluster.instances["node1"]

    root_namespace = f"clickhouse_{uuid.uuid4()}"
    namespace_1 = f"{root_namespace}_testA_A"
    namespace_2 = f"{root_namespace}_testB_B"
    namespace_1_tables = ["tableA", "tableB"]
    namespace_2_tables = ["tableC", "tableD"]

    catalog = load_catalog_impl(started_cluster)

    for namespace in [namespace_1, namespace_2]:
        catalog.create_namespace(namespace)

    for namespace in [namespace_1, namespace_2]:
        assert len(catalog.list_tables(namespace)) == 0

    create_clickhouse_glue_database(started_cluster, node, CATALOG_NAME)

    tables_list = ""
    for table in namespace_1_tables:
        create_table(catalog, namespace_1, table)
        if len(tables_list) > 0:
            tables_list += "\n"
        tables_list += f"{namespace_1}.{table}"

    for table in namespace_2_tables:
        create_table(catalog, namespace_2, table)
        if len(tables_list) > 0:
            tables_list += "\n"
        tables_list += f"{namespace_2}.{table}"

    assert (
        tables_list
        == node.query(
            f"SELECT name FROM system.tables WHERE database = '{CATALOG_NAME}' and name ILIKE '{root_namespace}%' ORDER BY name"
        ).strip()
    )
    node.restart_clickhouse()
    assert (
        tables_list
        == node.query(
            f"SELECT name FROM system.tables WHERE database = '{CATALOG_NAME}' and name ILIKE '{root_namespace}%' ORDER BY name"
        ).strip()
    )

    expected = DEFAULT_CREATE_TABLE.format(CATALOG_NAME, namespace_2, "tableC")
    print("Expected", expected)
    print("Got", node.query(f"SHOW CREATE TABLE {CATALOG_NAME}.`{namespace_2}.tableC`"))
    assert expected == node.query(
        f"SHOW CREATE TABLE {CATALOG_NAME}.`{namespace_2}.tableC`"
    )
    assert int(node.query(f"SELECT count() FROM system.iceberg_history WHERE database = '{CATALOG_NAME}' and table ilike '%{root_namespace}%'").strip()) == 0


def test_select(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_list_tables_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    namespaces_to_create = [
        root_namespace,
        f"{root_namespace}_A",
        f"{root_namespace}_B",
        f"{root_namespace}_C",
    ]

    catalog = load_catalog_impl(started_cluster)

    for namespace in namespaces_to_create:
        catalog.create_namespace(namespace)
        assert len(catalog.list_tables(namespace)) == 0

    for namespace in namespaces_to_create:
        table = create_table(catalog, namespace, table_name)

        num_rows = 10
        df = generate_arrow_data(num_rows)
        table.append(df)

        create_clickhouse_glue_database(started_cluster, node, CATALOG_NAME)

        expected = DEFAULT_CREATE_TABLE.format(CATALOG_NAME, namespace, table_name)
        assert expected == node.query(
            f"SHOW CREATE TABLE {CATALOG_NAME}.`{namespace}.{table_name}`"
        )

        assert num_rows == int(
            node.query(f"SELECT count() FROM {CATALOG_NAME}.`{namespace}.{table_name}`")
        )

    assert int(node.query(f"SELECT count() FROM system.iceberg_history WHERE database = '{CATALOG_NAME}' and table ilike '%{root_namespace}%'").strip()) == 4


def test_hide_sensitive_info(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_hide_sensitive_info_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    namespace = f"{root_namespace}_A"
    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)

    create_table(catalog, namespace, table_name)

    create_clickhouse_glue_database(
        started_cluster,
        node,
        CATALOG_NAME,
        additional_settings={
            "aws_access_key_id": "SECRET_1",
            "aws_secret_access_key": "SECRET_2",
        },
    )
    assert "SECRET_1" not in node.query(f"SHOW CREATE DATABASE {CATALOG_NAME}")
    assert "SECRET_2" not in node.query(f"SHOW CREATE DATABASE {CATALOG_NAME}")



def test_select_after_rename(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_list_tables_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    namespaces_to_create = [
        root_namespace,
        f"{root_namespace}_A",
        f"{root_namespace}_B",
        f"{root_namespace}_C",
    ]

    catalog = load_catalog_impl(started_cluster)

    for namespace in namespaces_to_create:
        catalog.create_namespace(namespace)
        assert len(catalog.list_tables(namespace)) == 0

    for namespace in namespaces_to_create:
        table = create_table(catalog, namespace, table_name)

        num_rows = 10
        df = generate_arrow_data(num_rows)
        table.append(df)

        create_clickhouse_glue_database(started_cluster, node, CATALOG_NAME)

        expected = DEFAULT_CREATE_TABLE.format(CATALOG_NAME, namespace, table_name)
        assert expected == node.query(
            f"SHOW CREATE TABLE {CATALOG_NAME}.`{namespace}.{table_name}`"
        )

        with table.update_schema() as update:
            update.rename_column("bid", "new_bid")

        print(node.query(f"SELECT * FROM {CATALOG_NAME}.`{namespace}.{table_name}`"))

def test_non_existing_tables(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_non_existing_tables_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    namespaces_to_create = [
        root_namespace,
        f"{root_namespace}_A",
    ]

    catalog = load_catalog_impl(started_cluster)

    for namespace in namespaces_to_create:
        catalog.create_namespace(namespace)

    for namespace in namespaces_to_create:
        table = create_table(catalog, namespace, table_name)

        num_rows = 10
        df = generate_arrow_data(num_rows)
        table.append(df)

        create_clickhouse_glue_database(started_cluster, node, CATALOG_NAME)

        expected = DEFAULT_CREATE_TABLE.format(CATALOG_NAME, namespace, table_name)
        assert expected == node.query(
            f"SHOW CREATE TABLE {CATALOG_NAME}.`{namespace}.{table_name}`"
        )

        try:
            node.query(f"SHOW CREATE TABLE {CATALOG_NAME}.`{namespace}.wrong_table_name`")
        except Exception as e:
            assert "DB::Exception: Table" in str(e)
            assert "doesn't exist" in str(e)

        try:
            node.query(f"SHOW CREATE TABLE {CATALOG_NAME}.`fake_namespace.wrong_table_name`")
        except Exception as e:
            assert "DB::Exception: Table" in str(e)
            assert "doesn't exist" in str(e)
