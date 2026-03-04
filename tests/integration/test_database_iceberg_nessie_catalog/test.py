import json
import random
import requests
import time
import uuid
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pytest
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    DoubleType,
    NestedField,
    StringType,
    TimestampType,
)

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key, minio_access_key
from helpers.test_tools import TSV, csv_compare

BASE_URL_LOCAL = "http://localhost:19120/iceberg/"
BASE_URL = "http://nessie:19120/iceberg/"
CATALOG_NAME = "demo"
WAREHOUSE_NAME = "warehouse"

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
        field_type=StringType(),  # Simplified from struct for compatibility
        required=False,
    ),
)

DEFAULT_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
    )
)

DEFAULT_SORT_ORDER = SortOrder(SortField(source_id=2, transform=IdentityTransform()))


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
        partition_spec=partition_spec,
        sort_order=sort_order,
        properties={"write.metadata.compression-codec": "none"},
    )


def create_clickhouse_iceberg_database(
    started_cluster, node, name, additional_settings={}
):
    settings = {
        "catalog_type": "rest",
        "warehouse": "warehouse", 
        "storage_endpoint": "http://minio:9000/warehouse-rest",
    }

    settings.update(additional_settings)

    node.query(
        f"""
DROP DATABASE IF EXISTS {name};
SET allow_experimental_database_iceberg=true;
CREATE DATABASE {name} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
    """
    )
    show_result = node.query(f"SHOW DATABASE {name}")
    assert minio_secret_key not in show_result
    assert "HIDDEN" in show_result


def load_catalog_impl(started_cluster):
    minio_ip = started_cluster.get_instance_ip('minio')
    s3_endpoint = f"http://{minio_ip}:9002"

    return RestCatalog(
        name="my_catalog",
        warehouse=WAREHOUSE_NAME,
        uri=BASE_URL_LOCAL,
        token="dummy",
        **{
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=["configs/backups.xml"],
            user_configs=[],
            stay_alive=True,
            with_iceberg_catalog=True,
            extra_parameters={
                "docker_compose_file_name": "docker_compose_iceberg_nessie_catalog.yml"
            },
        )

        cluster.start()

        time.sleep(15)

        yield cluster

    finally:
        cluster.shutdown()

def test_list_tables(started_cluster):
    node = started_cluster.instances["node1"]

    namespace_prefix = f"clickhouse_{uuid.uuid4().hex[:8]}"
    namespace_1 = f"{namespace_prefix}_testA"
    namespace_2 = f"{namespace_prefix}_testB"
    namespace_1_tables = ["tableA", "tableB"]
    namespace_2_tables = ["tableC", "tableD"]

    catalog = load_catalog_impl(started_cluster)

    for namespace in [namespace_1, namespace_2]:
        catalog.create_namespace((namespace,))

    for namespace in [namespace_1, namespace_2]:
        assert len(catalog.list_tables((namespace,))) == 0

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    tables_list = []
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=DoubleType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )

    for table in namespace_1_tables:
        catalog.create_table(
            (namespace_1, table),
            schema=schema,
            properties={"write.metadata.compression-codec": "none"},
        )
        tables_list.append(f"{namespace_1}.{table}")

    for table in namespace_2_tables:
        catalog.create_table(
            (namespace_2, table),
            schema=schema,
            properties={"write.metadata.compression-codec": "none"},
        )
        tables_list.append(f"{namespace_2}.{table}")

    # Verify tables were created via PyIceberg
    assert len(catalog.list_tables((namespace_1,))) == 2
    assert len(catalog.list_tables((namespace_2,))) == 2

    assert (
        "\n".join(sorted(tables_list))
        == node.query(
            f"SELECT name FROM system.tables WHERE database = '{CATALOG_NAME}' and name ILIKE '{namespace_prefix}%' ORDER BY name SETTINGS show_data_lake_catalogs_in_system_tables = true"
        ).strip()
    )


def test_select(started_cluster):
    """Test select operations with Nessie catalog"""

    node = started_cluster.instances["node1"]

    catalog = load_catalog_impl(started_cluster)

    test_ref = f"test_select_{uuid.uuid4().hex[:8]}"
    test_namespace = (f"{test_ref}_namespace",)
    
    catalog.create_namespace(test_namespace)

    test_table_name = f"{test_ref}_table"
    test_table_identifier = (test_namespace[0], test_table_name)

    try:
        existing_tables = catalog.list_tables(namespace=test_namespace)

        if test_table_identifier in existing_tables:
            catalog.drop_table(test_table_identifier)
    except Exception as e:
        pass

    simple_schema = Schema(
        NestedField(field_id=1, name="id", field_type=DoubleType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )

    table = catalog.create_table(
        test_table_identifier,
        schema=simple_schema,
        properties={"write.metadata.compression-codec": "none"},
    )

    df = pd.DataFrame(
        {
            "id": [1.0, 2.0, 3.0, 4.0, 5.0],
            "data": ["hello", "world", "from", "nessie", "test"],
        }
    )

    pa_df = pa.Table.from_pandas(df)

    table.append(pa_df)

    scan_result = table.scan().to_pandas()

    assert len(scan_result) == 5
    assert list(scan_result["id"]) == [1.0, 2.0, 3.0, 4.0, 5.0]
    assert list(scan_result["data"]) == ["hello", "world", "from", "nessie", "test"]

    namespaces = catalog.list_namespaces()

    tables = catalog.list_tables(namespace=test_namespace)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    assert int(node.query(f"SELECT count(*) FROM {CATALOG_NAME}.`{test_namespace[0]}.{test_table_name}`")) == len(scan_result)

    result = node.query(f"SELECT id, data FROM {CATALOG_NAME}.`{test_namespace[0]}.{test_table_name}` ORDER BY id FORMAT TSV")
    expected = TSV("""
1   hello
2	world
3	from
4	nessie
5	test
""")
    assert csv_compare(result, expected), f"got\n{result}\nwant\n{expected}"
    

def test_hide_sensitive_info(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_hide_sensitive_info_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    namespace = (root_namespace,)
    catalog = load_catalog_impl(started_cluster)

    catalog.create_namespace(namespace)

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=DoubleType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    catalog.create_table(
        (namespace[0], table_name),
        schema=schema,
        properties={"write.metadata.compression-codec": "none"},
    )

    create_clickhouse_iceberg_database(
        started_cluster,
        node,
        CATALOG_NAME,
        additional_settings={"catalog_credential": "SECRET_1"},
    )
    show_result = node.query(f"SHOW CREATE DATABASE {CATALOG_NAME}")
    assert "SECRET_1" not in show_result
    assert minio_secret_key not in show_result

    create_clickhouse_iceberg_database(
        started_cluster,
        node,
        CATALOG_NAME,
        additional_settings={"auth_header": "SECRET_2"},
    )
    show_result = node.query(f"SHOW CREATE DATABASE {CATALOG_NAME}")
    assert "SECRET_2" not in show_result
    assert minio_secret_key not in show_result

def test_tables_with_same_location(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_tables_with_same_location_{uuid.uuid4()}"
    namespace = f"{test_ref}_namespace"
    catalog = load_catalog_impl(started_cluster)

    table_name = f"{test_ref}_table"
    table_name_2 = f"{test_ref}_table_2"

    catalog.create_namespace((namespace,))
    table = create_table(catalog, namespace, table_name)
    table_2 = create_table(catalog, namespace, table_name_2)

    def record(key):
        return {
            "datetime": datetime.now(),
            "symbol": str(key),
            "bid": round(random.uniform(100, 200), 2),
            "ask": round(random.uniform(200, 300), 2),
            "details": "created_by Alice Smith",  # Simplified from nested dict
        }

    data = [record('aaa') for _ in range(3)]
    df = pa.Table.from_pylist(data)
    table.append(df)

    data = [record('bbb') for _ in range(3)]
    df = pa.Table.from_pylist(data)
    table_2.append(df)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    assert 'aaa\naaa\naaa' == node.query(f"SELECT symbol FROM {CATALOG_NAME}.`{namespace}.{table_name}`").strip()
    assert 'bbb\nbbb\nbbb' == node.query(f"SELECT symbol FROM {CATALOG_NAME}.`{namespace}.{table_name_2}`").strip()

def test_backup_database(started_cluster):
    node = started_cluster.instances["node1"]
    create_clickhouse_iceberg_database(started_cluster, node, "backup_database")

    backup_id = uuid.uuid4().hex
    backup_name = f"File('/backups/test_backup_{backup_id}/')"

    node.query(f"BACKUP DATABASE backup_database TO {backup_name}")
    node.query("DROP DATABASE backup_database SYNC")
    assert "backup_database" not in node.query("SHOW DATABASES")

    node.query(f"RESTORE DATABASE backup_database FROM {backup_name}", settings={"allow_experimental_database_iceberg": 1})
    assert (
        node.query("SHOW CREATE DATABASE backup_database")
        == "CREATE DATABASE backup_database\\nENGINE = DataLakeCatalog(\\'http://nessie:19120/iceberg/\\', \\'minio\\', \\'[HIDDEN]\\')\\nSETTINGS catalog_type = \\'rest\\', warehouse = \\'warehouse\\', storage_endpoint = \\'http://minio:9000/warehouse-rest\\'\n"
    )

def test_timestamps(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_list_tables_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    test_namespace = (f"{test_ref}_namespace",)
    test_table_identifier = (test_namespace[0], table_name)
    
    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(test_namespace)

    simple_schema = Schema(
        NestedField(
            field_id=1, name="timestamp", field_type=TimestampType(), required=False
        ),
        NestedField(
            field_id=2, name="timestamptz", field_type=TimestampType(), required=False
        ),
    )
    
    table = catalog.create_table(
        test_table_identifier,
        schema=simple_schema,
        properties={"write.metadata.compression-codec": "none"},
    )

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    data = [
        {
            "timestamp": datetime(2024, 1, 1, hour=12, minute=0, second=0, microsecond=0),
            "timestamptz": datetime(2024, 1, 1, hour=12, minute=0, second=0, microsecond=0),
        }
    ]
    df = pa.Table.from_pylist(data)
    table.append(df)

    # Extract the table path from S3 location for ClickHouse Iceberg ENGINE configuration
    # 
    # The table metadata contains the full S3 URI which needs to be processed:
    # table.metadata.location:  s3://warehouse-rest/<test_namespace>/<test_table_uuid>
    # extracted_table_path: <test_namespace>/<test_table_uuid>
    
    table_metadata = table.metadata
    table_location = table_metadata.location
    if "warehouse-rest/" in table_location:
        extracted_table_path = table_location.split("warehouse-rest/")[1]

    result = node.query(f"SHOW CREATE TABLE {CATALOG_NAME}.`{test_namespace[0]}.{table_name}`")
    assert result == f"CREATE TABLE {CATALOG_NAME}.`{test_namespace[0]}.{table_name}`\\n(\\n    `timestamp` Nullable(DateTime64(6)),\\n    `timestamptz` Nullable(DateTime64(6))\\n)\\nENGINE = Iceberg(\\'http://minio:9000/warehouse-rest/{extracted_table_path}/\\', \\'minio\\', \\'[HIDDEN]\\')\n"
    assert node.query(f"SELECT * FROM {CATALOG_NAME}.`{test_namespace[0]}.{table_name}`") == "2024-01-01 12:00:00.000000\t2024-01-01 12:00:00.000000\n"

def test_insert(started_cluster):
    node = started_cluster.instances["node1"]

    catalog = load_catalog_impl(started_cluster)

    test_ref = f"test_insert_{uuid.uuid4().hex[:8]}"
    test_namespace = (f"{test_ref}_namespace",)

    catalog.create_namespace(test_namespace)

    test_table_name = f"{test_ref}_table"
    test_table_identifier = (test_namespace[0], test_table_name)

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=DoubleType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )

    table = catalog.create_table(
        test_table_identifier,
        schema=schema,
        properties={"write.metadata.compression-codec": "none"},
    )

    data = [
    {"id": float(i), "data": f"data_{i}"} for i in range(10)
    ]
    df = pa.Table.from_pylist(data)
    table.append(df)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    result = node.query(f"SELECT * FROM {CATALOG_NAME}.`{test_namespace[0]}.{test_table_name}` ORDER BY id")
    expected = "\n".join([f"{i}\tdata_{i}" for i in range(10)])
    assert result.strip() == expected

def test_create(started_cluster):
    node = started_cluster.instances["node1"]

    catalog = load_catalog_impl(started_cluster)

    test_ref = f"test_create_{uuid.uuid4().hex[:8]}"
    test_namespace = (f"{test_ref}_namespace",)

    catalog.create_namespace(test_namespace)

    test_table_name = f"{test_ref}_table"
    test_table_identifier = (test_namespace[0], test_table_name)

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=DoubleType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )

    catalog.create_table(
        test_table_identifier,
        schema=schema,
        properties={"write.metadata.compression-codec": "none"},
    )

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    result = node.query(f"SHOW TABLES FROM {CATALOG_NAME}")
    assert test_table_name in result

def test_drop_table(started_cluster):
    node = started_cluster.instances["node1"]

    catalog = load_catalog_impl(started_cluster)

    test_ref = f"test_drop_table_{uuid.uuid4().hex[:8]}"
    test_namespace = (f"{test_ref}_namespace",)

    catalog.create_namespace(test_namespace)

    test_table_name = f"{test_ref}_table"
    test_table_identifier = (test_namespace[0], test_table_name)

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=DoubleType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )

    catalog.create_table(
        test_table_identifier,
        schema=schema,
        properties={"write.metadata.compression-codec": "none"},
    )

    catalog.drop_table(test_table_identifier)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    result = node.query(f"SHOW TABLES FROM {CATALOG_NAME}")
    assert test_table_name not in result