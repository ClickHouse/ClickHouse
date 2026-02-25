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
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    DoubleType,
    FloatType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType
)

from helpers.cluster import ClickHouseCluster, ClickHouseInstance, is_arm
from helpers.config_cluster import minio_secret_key, minio_access_key
from helpers.s3_tools import get_file_contents, list_s3_objects, prepare_s3_bucket
from helpers.test_tools import TSV, csv_compare
from helpers.config_cluster import minio_secret_key
from helpers.network import PartitionManager
from helpers.client import QueryRuntimeException

BASE_URL = "http://rest:8181/v1"
BASE_URL_LOCAL = "http://localhost:8182/v1"
BASE_URL_LOCAL_RAW = "http://localhost:8182"

CATALOG_NAME = "demo"

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
)

DEFAULT_CREATE_TABLE = "CREATE TABLE {}.`{}.{}`\\n(\\n    `datetime` Nullable(DateTime64(6)),\\n    `symbol` Nullable(String),\\n    `bid` Nullable(Float64),\\n    `ask` Nullable(Float64),\\n    `details` Tuple(created_by Nullable(String))\\n)\\nENGINE = Iceberg(\\'http://minio:9000/warehouse-rest/data/\\', \\'minio\\', \\'[HIDDEN]\\')\n"

DEFAULT_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
    )
)

DEFAULT_SORT_ORDER = SortOrder(SortField(source_id=2, transform=IdentityTransform()))


def list_namespaces():
    response = requests.get(f"{BASE_URL_LOCAL}/namespaces")
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to list namespaces: {response.status_code}")


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


def generate_record():
    return {
        "datetime": datetime.now(),
        "symbol": str("kek"),
        "bid": round(random.uniform(100, 200), 2),
        "ask": round(random.uniform(200, 300), 2),
        "details": {"created_by": "Alice Smith"},
    }


def create_clickhouse_iceberg_database(
    started_cluster, node, name, additional_settings={}
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
CREATE DATABASE {name} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
    """
    )
    show_result = node.query(f"SHOW DATABASE {name}")
    assert minio_secret_key not in show_result
    assert "HIDDEN" in show_result


def create_clickhouse_iceberg_table(
    started_cluster, node, database_name, table_name, schema, additional_settings={}
):
    settings = {
        "storage_catalog_type": "rest",
        "storage_warehouse": "demo",
        "object_storage_endpoint": "http://minio:9000/warehouse-rest",
        "storage_region": "us-east-1",
        "storage_catalog_url" : BASE_URL,
    }

    settings.update(additional_settings)

    node.query(
        f"""
SET allow_experimental_database_iceberg=true;
SET write_full_path_in_iceberg_metadata=1;
CREATE TABLE {CATALOG_NAME}.`{database_name}.{table_name}` {schema} ENGINE = IcebergS3('http://minio:9000/warehouse-rest/{table_name}/', '{minio_access_key}', '{minio_secret_key}')
SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
    """
    )

def drop_clickhouse_iceberg_table(
    node, database_name, table_name, if_exists=False
):
    if if_exists:
        node.query(
            f"""
    DROP TABLE IF EXISTS {CATALOG_NAME}.`{database_name}.{table_name}`
        """
        )
    else:
        node.query(
            f"""
    DROP TABLE {CATALOG_NAME}.`{database_name}.{table_name}`
        """
        )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=["configs/backups.xml","configs/cluster.xml"],
            user_configs=[],
            stay_alive=True,
            with_iceberg_catalog=True,
        )

        cluster.add_instance(
            "node2",
            main_configs=["configs/backups.xml","configs/cluster.xml"],
            user_configs=[],
            stay_alive=True,
            with_iceberg_catalog=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        # TODO: properly wait for container
        time.sleep(10)

        yield cluster

    finally:
        cluster.shutdown()


def test_list_tables(started_cluster):
    node = started_cluster.instances["node1"]

    root_namespace = f"clickhouse_{uuid.uuid4()}"
    namespace_1 = f"{root_namespace}.testA.A"
    namespace_2 = f"{root_namespace}.testB.B"
    namespace_1_tables = ["tableA", "tableB"]
    namespace_2_tables = ["tableC", "tableD"]

    catalog = load_catalog_impl(started_cluster)

    for namespace in [namespace_1, namespace_2]:
        catalog.create_namespace(namespace)

    found = False
    for namespace_list in list_namespaces()["namespaces"]:
        if root_namespace == namespace_list[0]:
            found = True
            break
    assert found

    found = False
    for namespace_list in catalog.list_namespaces():
        if root_namespace == namespace_list[0]:
            found = True
            break
    assert found

    for namespace in [namespace_1, namespace_2]:
        assert len(catalog.list_tables(namespace)) == 0

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

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
            f"SELECT name FROM system.tables WHERE database = '{CATALOG_NAME}' and name ILIKE '{root_namespace}%' ORDER BY name SETTINGS show_data_lake_catalogs_in_system_tables = true"
        ).strip()
    )
    node.restart_clickhouse()
    assert (
        tables_list
        == node.query(
            f"SELECT name FROM system.tables WHERE database = '{CATALOG_NAME}' and name ILIKE '{root_namespace}%' ORDER BY name SETTINGS show_data_lake_catalogs_in_system_tables = true"
        ).strip()
    )

    expected = DEFAULT_CREATE_TABLE.format(CATALOG_NAME, namespace_2, "tableC")
    assert expected == node.query(
        f"SHOW CREATE TABLE {CATALOG_NAME}.`{namespace_2}.tableC`"
    )


def test_many_namespaces(started_cluster):
    node = started_cluster.instances["node1"]
    root_namespace_1 = f"A_{uuid.uuid4()}"
    root_namespace_2 = f"B_{uuid.uuid4()}"
    namespaces = [
        f"{root_namespace_1}",
        f"{root_namespace_1}.B.C",
        f"{root_namespace_1}.B.C.D",
        f"{root_namespace_1}.B.C.D.E",
        f"{root_namespace_2}",
        f"{root_namespace_2}.C",
        f"{root_namespace_2}.CC",
    ]
    tables = ["A", "B", "C"]
    catalog = load_catalog_impl(started_cluster)

    for namespace in namespaces:
        catalog.create_namespace(namespace)
        for table in tables:
            create_table(catalog, namespace, table)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    for namespace in namespaces:
        for table in tables:
            table_name = f"{namespace}.{table}"
            assert int(
                node.query(
                    f"SELECT count() FROM system.tables WHERE database = '{CATALOG_NAME}' and name = '{table_name}' SETTINGS show_data_lake_catalogs_in_system_tables = true"
                )
            )


def test_select(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_list_tables_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    namespace = f"{root_namespace}.A.B.C"
    namespaces_to_create = [
        root_namespace,
        f"{root_namespace}.A",
        f"{root_namespace}.A.B",
        f"{root_namespace}.A.B.C",
    ]

    catalog = load_catalog_impl(started_cluster)

    for namespace in namespaces_to_create:
        catalog.create_namespace(namespace)
        assert len(catalog.list_tables(namespace)) == 0

    table = create_table(catalog, namespace, table_name)

    num_rows = 10
    data = [generate_record() for _ in range(num_rows)]
    df = pa.Table.from_pylist(data)
    table.append(df)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    expected = DEFAULT_CREATE_TABLE.format(CATALOG_NAME, namespace, table_name)
    assert expected == node.query(
        f"SHOW CREATE TABLE {CATALOG_NAME}.`{namespace}.{table_name}`"
    )

    assert num_rows == int(
        node.query(f"SELECT count() FROM {CATALOG_NAME}.`{namespace}.{table_name}`")
    )

    assert int(node.query(f"SELECT count() FROM system.iceberg_history WHERE table = '{namespace}.{table_name}' and database = '{CATALOG_NAME}'").strip()) == 1


def test_hide_sensitive_info(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_hide_sensitive_info_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    namespace = f"{root_namespace}.A"
    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)

    table = create_table(catalog, namespace, table_name)

    create_clickhouse_iceberg_database(
        started_cluster,
        node,
        CATALOG_NAME,
        additional_settings={"catalog_credential": "SECRET_1"},
    )
    assert "SECRET_1" not in node.query(f"SHOW CREATE DATABASE {CATALOG_NAME}")

    create_clickhouse_iceberg_database(
        started_cluster,
        node,
        CATALOG_NAME,
        additional_settings={"auth_header": "SECRET_2"},
    )
    assert "SECRET_2" not in node.query(f"SHOW CREATE DATABASE {CATALOG_NAME}")


def test_tables_with_same_location(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_tables_with_same_location_{uuid.uuid4()}"
    namespace = f"{test_ref}_namespace"
    catalog = load_catalog_impl(started_cluster)

    table_name = f"{test_ref}_table"
    table_name_2 = f"{test_ref}_table_2"

    catalog.create_namespace(namespace)
    table = create_table(catalog, namespace, table_name)
    table_2 = create_table(catalog, namespace, table_name_2)

    def record(key):
        return {
            "datetime": datetime.now(),
            "symbol": str(key),
            "bid": round(random.uniform(100, 200), 2),
            "ask": round(random.uniform(200, 300), 2),
            "details": {"created_by": "Alice Smith"},
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

    node.query(f"RESTORE DATABASE backup_database FROM {backup_name}", settings={"allow_database_iceberg": 1})
    assert (
        node.query("SHOW CREATE DATABASE backup_database")
        == "CREATE DATABASE backup_database\\nENGINE = DataLakeCatalog(\\'http://rest:8181/v1\\', \\'minio\\', \\'[HIDDEN]\\')\\nSETTINGS catalog_type = \\'rest\\', warehouse = \\'demo\\', storage_endpoint = \\'http://minio:9000/warehouse-rest\\'\n"
    )


def test_non_existing_tables(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_list_tables_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    namespace = f"{root_namespace}.A.B.C"
    namespaces_to_create = [
        root_namespace,
        f"{root_namespace}.A",
        f"{root_namespace}.A.B",
        f"{root_namespace}.A.B.C",
    ]

    catalog = load_catalog_impl(started_cluster)

    for namespace in namespaces_to_create:
        catalog.create_namespace(namespace)
        assert len(catalog.list_tables(namespace)) == 0

    table = create_table(catalog, namespace, table_name)

    num_rows = 10
    data = [generate_record() for _ in range(num_rows)]
    df = pa.Table.from_pylist(data)
    table.append(df)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    expected = DEFAULT_CREATE_TABLE.format(CATALOG_NAME, namespace, table_name)
    assert expected == node.query(
        f"SHOW CREATE TABLE {CATALOG_NAME}.`{namespace}.{table_name}`"
    )

    try:
        node.query(
            f"SHOW CREATE TABLE {CATALOG_NAME}.`{namespace}.qweqwe`"
        )
    except Exception as e:
        assert "DB::Exception: Table" in str(e)
        assert "doesn't exist" in str(e)

    try:
        node.query(
            f"SHOW CREATE TABLE {CATALOG_NAME}.`qweqwe.qweqwe`"
        )
    except Exception as e:
        assert "DB::Exception: Table" in str(e)
        assert "doesn't exist" in str(e)


def test_timestamps(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_list_tables_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(root_namespace)

    schema = Schema(
        NestedField(
            field_id=1, name="timestamp", field_type=TimestampType(), required=False
        ),
        NestedField(
            field_id=2,
            name="timestamptz",
            field_type=TimestamptzType(),
            required=False,
        ),
    )
    table = create_table(catalog, root_namespace, table_name, schema)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    data = [
        {
            "timestamp": datetime(2024, 1, 1, hour=12, minute=0, second=0, microsecond=0),
            "timestamptz": datetime(
                2024,
                1,
                1,
                hour=12,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=pytz.timezone("UTC"),
            )
        }
    ]
    df = pa.Table.from_pylist(data)
    table.append(df)

    assert node.query(f"SHOW CREATE TABLE {CATALOG_NAME}.`{root_namespace}.{table_name}`") == f"CREATE TABLE {CATALOG_NAME}.`{root_namespace}.{table_name}`\\n(\\n    `timestamp` Nullable(DateTime64(6)),\\n    `timestamptz` Nullable(DateTime64(6, \\'UTC\\'))\\n)\\nENGINE = Iceberg(\\'http://minio:9000/warehouse-rest/data/\\', \\'minio\\', \\'[HIDDEN]\\')\n"
    assert node.query(f"SELECT * FROM {CATALOG_NAME}.`{root_namespace}.{table_name}`") == "2024-01-01 12:00:00.000000\t2024-01-01 12:00:00.000000\n"


def test_insert(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_list_tables_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(root_namespace)

    create_table(catalog, root_namespace, table_name, DEFAULT_SCHEMA, PartitionSpec(), DEFAULT_SORT_ORDER)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)
    node.query(f"INSERT INTO {CATALOG_NAME}.`{root_namespace}.{table_name}` VALUES (NULL, 'AAPL', 193.24, 193.31, tuple('bot'));", settings={"allow_insert_into_iceberg": 1, 'write_full_path_in_iceberg_metadata': 1})
    catalog.load_table(f"{root_namespace}.{table_name}")
    assert node.query(f"SELECT * FROM {CATALOG_NAME}.`{root_namespace}.{table_name}`") == "\\N\tAAPL\t193.24\t193.31\t('bot')\n"

    node.query(f"INSERT INTO {CATALOG_NAME}.`{root_namespace}.{table_name}` VALUES (NULL, 'Pavel Ivanov (pudge1000-7) pereezhai v amsterdam', 193.24, 193.31, tuple('bot'));", settings={"allow_insert_into_iceberg": 1, 'write_full_path_in_iceberg_metadata': 1})
    assert node.query(f"SELECT * FROM {CATALOG_NAME}.`{root_namespace}.{table_name}` ORDER BY ALL") == "\\N\tAAPL\t193.24\t193.31\t('bot')\n\\N\tPavel Ivanov (pudge1000-7) pereezhai v amsterdam\t193.24\t193.31\t('bot')\n"


def test_create(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_list_tables_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)
    create_clickhouse_iceberg_table(started_cluster, node, root_namespace, table_name, "(x String)")
    node.query(f"INSERT INTO {CATALOG_NAME}.`{root_namespace}.{table_name}` VALUES ('AAPL');", settings={"allow_insert_into_iceberg": 1, 'write_full_path_in_iceberg_metadata': 1})
    assert node.query(f"SELECT * FROM {CATALOG_NAME}.`{root_namespace}.{table_name}`") == "AAPL\n"


def test_drop_table(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_list_tables_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    catalog = load_catalog_impl(started_cluster)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)
    create_clickhouse_iceberg_table(started_cluster, node, root_namespace, table_name, "(x String)")
    assert len(catalog.list_tables(root_namespace)) == 1

    drop_clickhouse_iceberg_table(node, root_namespace, table_name + "some_strange_non_exists_suffix", True)
    assert len(catalog.list_tables(root_namespace)) == 1

    drop_clickhouse_iceberg_table(node, root_namespace, table_name)
    assert len(catalog.list_tables(root_namespace)) == 0


def test_table_with_slash(started_cluster):
    node = started_cluster.instances["node1"]

    # pyiceberg at current moment (version 0.9.1) has a bug with table names with slashes
    # see https://github.com/apache/iceberg-python/issues/2462
    # so we need to encode it manually
    table_raw_suffix = "table/foo"
    table_encoded_suffix = "table%2Ffoo"

    test_ref = f"test_list_tables_{uuid.uuid4()}"
    table_name = f"{test_ref}_{table_raw_suffix}"
    table_encoded_name = f"{test_ref}_{table_encoded_suffix}"
    root_namespace = f"{test_ref}_namespace"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(root_namespace)

    create_table(catalog, root_namespace, table_name, DEFAULT_SCHEMA, PartitionSpec(), DEFAULT_SORT_ORDER)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)
    node.query(f"INSERT INTO {CATALOG_NAME}.`{root_namespace}.{table_encoded_name}` VALUES (NULL, 'AAPL', 193.24, 193.31, tuple('bot'));", settings={"allow_insert_into_iceberg": 1, 'write_full_path_in_iceberg_metadata': 1})
    assert node.query(f"SELECT * FROM {CATALOG_NAME}.`{root_namespace}.{table_encoded_name}`") == "\\N\tAAPL\t193.24\t193.31\t('bot')\n"


def test_cluster_select(started_cluster):
    node1 = started_cluster.instances["node1"]
    node2 = started_cluster.instances["node2"]

    test_ref = f"test_list_tables_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    catalog = load_catalog_impl(started_cluster)
    create_clickhouse_iceberg_database(started_cluster, node1, CATALOG_NAME)
    create_clickhouse_iceberg_database(started_cluster, node2, CATALOG_NAME)
    create_clickhouse_iceberg_table(started_cluster, node1, root_namespace, table_name, "(x String)")
    node1.query(f"INSERT INTO {CATALOG_NAME}.`{root_namespace}.{table_name}` VALUES ('pablo');", settings={"allow_insert_into_iceberg": 1, 'write_full_path_in_iceberg_metadata': 1})

    query_id = uuid.uuid4().hex
    assert node1.query(f"SELECT * FROM {CATALOG_NAME}.`{root_namespace}.{table_name}` SETTINGS parallel_replicas_for_cluster_engines=1, enable_parallel_replicas=2, cluster_for_parallel_replicas='cluster_simple'", query_id=query_id) == 'pablo\n'

    node1.query("SYSTEM FLUSH LOGS system.query_log")
    node2.query("SYSTEM FLUSH LOGS system.query_log")

    assert node1.query(f"SELECT Settings['parallel_replicas_for_cluster_engines'] AS parallel_replicas_for_cluster_engines FROM system.query_log WHERE query_id = '{query_id}' LIMIT 1;") == '1\n'

    for replica in [node1, node2]:
        cluster_secondary_queries = (
            replica.query(
                f"""
                SELECT query, type, is_initial_query, read_rows, read_bytes FROM system.query_log
                WHERE
                    type = 'QueryStart' AND
                    positionCaseInsensitive(query, 's3Cluster') != 0 AND
                    position(query, 'system.query_log') = 0 AND
                    NOT is_initial_query
            """
            )
            .strip()
            .split("\n")
        )
        assert len(cluster_secondary_queries) == 1

    assert node2.query(f"SELECT * FROM {CATALOG_NAME}.`{root_namespace}.{table_name}`", settings={"parallel_replicas_for_cluster_engines":1, 'enable_parallel_replicas': 2, 'cluster_for_parallel_replicas': 'cluster_simple', 'parallel_replicas_for_cluster_engines' : 1}) == 'pablo\n'
    
def test_not_specified_catalog_type(started_cluster):
    node = started_cluster.instances["node1"]
    settings = {
        "warehouse": "demo",
        "storage_endpoint": "http://minio:9000/warehouse-rest",
    }

    node.query(
        f"""
    DROP DATABASE IF EXISTS {CATALOG_NAME};
    SET allow_database_iceberg=true;
    SET write_full_path_in_iceberg_metadata=1;
    CREATE DATABASE {CATALOG_NAME} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
    SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
    """
    )
    assert "" == node.query(f"SHOW TABLES FROM {CATALOG_NAME}")

def test_gcs(started_cluster):
    node = started_cluster.instances["node1"]

    node.query("SYSTEM ENABLE FAILPOINT database_iceberg_gcs")
    node.query(
        f"""
        DROP DATABASE IF EXISTS {CATALOG_NAME};
        SET allow_database_iceberg = 1;
        """
    )

    with pytest.raises(Exception) as err:
        node.query(
            f"""
            CREATE DATABASE {CATALOG_NAME}
            ENGINE = DataLakeCatalog('{BASE_URL_DOCKER}', 'gcs', 'dummy')
            SETTINGS
                catalog_type = 'rest',
                warehouse = 'demo',
            """
        )
        assert "Google cloud storage converts to S3" in str(err.value)
