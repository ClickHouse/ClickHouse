import json
import logging
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pyarrow as pa
import pytest
import requests
import pytz
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    DoubleType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType
)

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key, minio_access_key
from helpers.client import QueryRuntimeException

BASE_URL = "http://rest:8181/v1"

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

DEFAULT_CREATE_TABLE = "CREATE TABLE {}.`{}.{}`\\n(\\n    `datetime` Nullable(DateTime64(6)),\\n    `symbol` Nullable(String),\\n    `bid` Nullable(Float64),\\n    `ask` Nullable(Float64),\\n    `details` Tuple(created_by Nullable(String))\\n)\\nENGINE = Iceberg(\\'http://minio1:9001/warehouse-rest/data/\\', \\'minio\\', \\'[HIDDEN]\\')\n"

DEFAULT_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
    )
)

DEFAULT_SORT_ORDER = SortOrder(SortField(source_id=2, transform=IdentityTransform()))


def list_namespaces(started_cluster):
    base_url_local = f"http://localhost:{started_cluster.iceberg_rest_catalog_port}/v1"
    response = requests.get(f"{base_url_local}/namespaces")
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to list namespaces: {response.status_code}")


def load_catalog_impl(started_cluster):
    base_url_local_raw = f"http://localhost:{started_cluster.iceberg_rest_catalog_port}"
    return load_catalog(
        CATALOG_NAME,
        **{
            "uri": base_url_local_raw,
            "type": "rest",
            "s3.endpoint": f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}",
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
        location="s3://warehouse-rest/data",
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
        "storage_endpoint": "http://minio1:9001/warehouse-rest",
    }

    settings.update(additional_settings)

    node.query(
        f"""
DROP DATABASE IF EXISTS {name};
CREATE DATABASE {name} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
    """,
        settings={
            "allow_database_iceberg": 1,
            "write_full_path_in_iceberg_metadata": 1,
        },
    )
    show_result = node.query(f"SHOW DATABASE {name}")
    assert minio_secret_key not in show_result
    assert "HIDDEN" in show_result

def create_clickhouse_iceberg_table(
    started_cluster, node, database_name, table_name, schema, additional_settings={}
):
    settings_suffix = "" if len(additional_settings) == 0 else f"SETTINGS {",".join((k+"="+repr(v) for k, v in additional_settings.items()))}"
    node.query(
        f"""
CREATE TABLE {CATALOG_NAME}.`{database_name}.{table_name}` {schema} ENGINE = IcebergS3('http://minio1:9001/warehouse-rest/{table_name}/', '{minio_access_key}', '{minio_secret_key}')
{settings_suffix}
    """,
        settings={
            "allow_experimental_database_iceberg": 1,
            "write_full_path_in_iceberg_metadata": 1,
        },
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
            main_configs=[
                "configs/backups.xml",
                "configs/cluster.xml",
                "configs/text_log.xml",
                "configs/mysql_port.xml",
            ],
            user_configs=[],
            stay_alive=True,
            with_iceberg_catalog=True,
        )

        cluster.add_instance(
            "node2",
            main_configs=[
                "configs/backups.xml",
                "configs/cluster.xml",
                "configs/text_log.xml",
            ],
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
    for namespace_list in list_namespaces(started_cluster)["namespaces"]:
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


def test_check_database(started_cluster):
    node = started_cluster.instances["node1"]

    root_namespace = f"clickhouse_{uuid.uuid4()}"
    namespace_1 = f"{root_namespace}.testA.A"
    namespace_2 = f"{root_namespace}.testB.B"
    namespace_1_tables = ["tableA", "tableB"]
    namespace_2_tables = ["tableC", "tableD"]

    catalog = load_catalog_impl(started_cluster)

    for namespace in [namespace_1, namespace_2]:
        catalog.create_namespace(namespace)

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

    node.query(
        f"CHECK DATABASE {CATALOG_NAME}"
    )

    try:
        node.query(
            "SYSTEM ENABLE FAILPOINT check_database_datalake_negative"
        )
    
        assert "fault when checking database" in node.query_and_get_error(
            f"CHECK DATABASE {CATALOG_NAME}"
        )
    finally:
        node.query(
            "SYSTEM DISABLE FAILPOINT check_database_datalake_negative"
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

    create_table(catalog, namespace, table_name)

    def check_secret_hidden(secret, additional_settings):
        settings = {
            "catalog_type": "rest",
            "warehouse": "demo",
            "storage_endpoint": "http://minio1:9001/warehouse-rest",
        }
        settings.update(additional_settings)

        node.query(f"DROP DATABASE IF EXISTS {CATALOG_NAME}")
        try:
            node.query(
                f"""CREATE DATABASE {CATALOG_NAME} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS {",".join((k + "=" + repr(v) for k, v in settings.items()))}""",
                settings={
                    "allow_database_iceberg": 1,
                    "write_full_path_in_iceberg_metadata": 1,
                },
            )
        except QueryRuntimeException as e:
            assert secret not in str(e), (
                f"Secret {secret!r} leaked into CREATE DATABASE error message"
            )
            return

        assert secret not in node.query(f"SHOW CREATE DATABASE {CATALOG_NAME}")

    check_secret_hidden("SECRET_1", {"catalog_credential": "id:SECRET_1"})
    check_secret_hidden("SECRET_2", {"auth_header": "Authorization: SECRET_2"})


def test_no_secrets_in_logs(started_cluster):
    node = started_cluster.instances["node1"]

    db_name = f"iceberg_query_log_{uuid.uuid4().hex}"
    root_namespace = f"log_check_ns_{uuid.uuid4().hex}"
    table_name = f"log_check_tbl_{uuid.uuid4().hex}"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(root_namespace)

    db_settings = {
        "catalog_type": "rest",
        "warehouse": "demo",
        "storage_endpoint": "http://minio1:9001/warehouse-rest",
    }
    qid_db = uuid.uuid4().hex
    node.query(f"DROP DATABASE IF EXISTS {db_name}")
    node.query(
        f"""CREATE DATABASE {db_name} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS {",".join((k + "=" + repr(v) for k, v in db_settings.items()))}""",
        query_id=qid_db,
        settings={
            "allow_database_iceberg": 1,
            "write_full_path_in_iceberg_metadata": 1,
        },
    )

    qid_table = uuid.uuid4().hex
    node.query(
        f"""CREATE TABLE {db_name}.`{root_namespace}.{table_name}` (x String) ENGINE = IcebergS3('http://minio1:9001/warehouse-rest/{table_name}/', '{minio_access_key}', '{minio_secret_key}')""",
        query_id=qid_table,
        settings={
            "allow_experimental_database_iceberg": 1,
            "write_full_path_in_iceberg_metadata": 1,
        },
    )

    qid_show_db = uuid.uuid4().hex
    show_db_result = node.query(
        f"SHOW CREATE DATABASE {db_name}", query_id=qid_show_db
    )
    assert minio_secret_key not in show_db_result
    assert "[HIDDEN]" in show_db_result

    qid_show_table = uuid.uuid4().hex
    show_table_result = node.query(
        f"SHOW CREATE TABLE {db_name}.`{root_namespace}.{table_name}`",
        query_id=qid_show_table,
    )
    assert minio_secret_key not in show_table_result
    assert "[HIDDEN]" in show_table_result

    node.query("SYSTEM FLUSH LOGS system.query_log")
    node.query("SYSTEM FLUSH LOGS system.text_log")

    for qid in (qid_db, qid_table, qid_show_db, qid_show_table):
        assert (
            int(
                node.query(
                    f"SELECT count() FROM system.query_log WHERE query_id = '{qid}' AND type = 'QueryFinish'"
                ).strip()
            )
            >= 1
        )
        query_text = node.query(
            f"SELECT arrayStringConcat(groupArray(query), '\\n') FROM system.query_log WHERE query_id = '{qid}' AND type = 'QueryFinish'"
        ).strip()
        assert minio_secret_key not in query_text

    text_log_rows = node.query(
        f"""
SELECT message, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10
FROM system.text_log
WHERE query_id IN ('{qid_db}', '{qid_table}', '{qid_show_db}', '{qid_show_table}')
FORMAT JSONEachRow
"""
    ).strip()
    assert text_log_rows
    for line in text_log_rows.split("\n"):
        row = json.loads(line)
        for val in row.values():
            if isinstance(val, str):
                assert minio_secret_key not in val


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
        == "CREATE DATABASE backup_database\\nENGINE = DataLakeCatalog(\\'http://rest:8181/v1\\', \\'minio\\', \\'[HIDDEN]\\')\\nSETTINGS catalog_type = \\'rest\\', warehouse = \\'demo\\', storage_endpoint = \\'http://minio1:9001/warehouse-rest\\'\n"
    )


def test_restore_database_replace_external_to_null(started_cluster):
    node = started_cluster.instances["node1"]
    db_name = "backup_database_null"
    create_clickhouse_iceberg_database(started_cluster, node, db_name)

    backup_id = uuid.uuid4().hex
    backup_name = f"File('/backups/test_backup_{backup_id}/')"

    node.query(f"BACKUP DATABASE {db_name} TO {backup_name}")
    node.query(f"DROP DATABASE {db_name} SYNC")
    assert db_name not in node.query("SHOW DATABASES")

    node.query(
        f"RESTORE DATABASE {db_name} FROM {backup_name}",
        settings={
            "restore_replace_external_engines_to_null": 1,
            "restore_replace_external_table_functions_to_null": 1,
            "restore_replace_external_dictionary_source_to_null": 1,
        },
    )
    assert db_name not in node.query("SHOW DATABASES")


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

    assert node.query(f"SHOW CREATE TABLE {CATALOG_NAME}.`{root_namespace}.{table_name}`") == f"CREATE TABLE {CATALOG_NAME}.`{root_namespace}.{table_name}`\\n(\\n    `timestamp` Nullable(DateTime64(6)),\\n    `timestamptz` Nullable(DateTime64(6, \\'UTC\\'))\\n)\\nENGINE = Iceberg(\\'http://minio1:9001/warehouse-rest/data/\\', \\'minio\\', \\'[HIDDEN]\\')\n"
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

    load_catalog_impl(started_cluster)
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
                """
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

    assert node2.query(f"SELECT * FROM {CATALOG_NAME}.`{root_namespace}.{table_name}`", settings={"parallel_replicas_for_cluster_engines": 1, "enable_parallel_replicas": 2, "cluster_for_parallel_replicas": "cluster_simple"}) == 'pablo\n'


def test_used_storages_in_query_log(started_cluster):
    node1 = started_cluster.instances["node1"]
    node2 = started_cluster.instances["node2"]

    test_ref = f"test_query_log_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    load_catalog_impl(started_cluster)
    create_clickhouse_iceberg_database(started_cluster, node1, CATALOG_NAME)
    create_clickhouse_iceberg_database(started_cluster, node2, CATALOG_NAME)
    create_clickhouse_iceberg_table(
        started_cluster, node1, root_namespace, table_name, "(x String)"
    )
    node1.query(
        f"INSERT INTO {CATALOG_NAME}.`{root_namespace}.{table_name}` VALUES ('test_log');",
        settings={
            "allow_insert_into_iceberg": 1,
            "write_full_path_in_iceberg_metadata": 1,
        },
    )

    query_id_non_cluster = uuid.uuid4().hex
    node1.query(
        f"SELECT * FROM {CATALOG_NAME}.`{root_namespace}.{table_name}`",
        query_id=query_id_non_cluster,
    )

    query_id_cluster = uuid.uuid4().hex
    node1.query(
        f"SELECT * FROM {CATALOG_NAME}.`{root_namespace}.{table_name}`"
        f" SETTINGS parallel_replicas_for_cluster_engines=1,"
        f" enable_parallel_replicas=2,"
        f" cluster_for_parallel_replicas='cluster_simple'",
        query_id=query_id_cluster,
    )

    node1.query("SYSTEM FLUSH LOGS")

    result_non_cluster = node1.query(
        f"SELECT used_storages FROM system.query_log"
        f" WHERE query_id = '{query_id_non_cluster}' AND type = 'QueryFinish'"
    ).strip()
    assert (
        "'IcebergS3'" in result_non_cluster
    ), f"Non-cluster: expected IcebergS3 in used_storages, got {result_non_cluster}"

    result_cluster = node1.query(
        f"SELECT used_storages FROM system.query_log"
        f" WHERE query_id = '{query_id_cluster}' AND type = 'QueryFinish'"
    ).strip()
    assert (
        "'IcebergS3'" in result_cluster
    ), f"Cluster: expected IcebergS3 in used_storages, got {result_cluster}"


def test_not_specified_catalog_type(started_cluster):
    node = started_cluster.instances["node1"]
    settings = {
        "warehouse": "demo",
        "storage_endpoint": "http://minio1:9001/warehouse-rest",
    }

    node.query(f"DROP DATABASE IF EXISTS {CATALOG_NAME}")

    with pytest.raises(QueryRuntimeException) as exc_info:
        node.query(
            f"""CREATE DATABASE {CATALOG_NAME} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS {",".join((k + "=" + repr(v) for k, v in settings.items()))}""",
            settings={
                "allow_database_iceberg": 1,
                "write_full_path_in_iceberg_metadata": 1,
            },
        )
    message = str(exc_info.value)
    assert "Unspecified catalog type" in message, message
    assert "Code: 36" in message, message


def test_system_tables_with_nullptr_table(started_cluster):
    """
    Test that querying system.tables does not crash when DataLake database
    returns nullptr for some tables (e.g. when table metadata fetch fails).
    Reproduces: https://github.com/ClickHouse/clickhouse-core-incidents/issues/1434
    """
    node = started_cluster.instances["node1"]

    root_namespace = f"clickhouse_{uuid.uuid4()}"
    namespace = f"{root_namespace}_test_nullptr"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)

    table_name = "test_table"
    create_table(catalog, namespace, table_name)

    num_rows = 5
    arrow_data = pa.table(
        {
            "datetime": [datetime.now() for _ in range(num_rows)],
            "symbol": [f"sym_{i}" for i in range(num_rows)],
            "bid": [float(i) for i in range(num_rows)],
            "ask": [float(i + 1) for i in range(num_rows)],
            "details": [{"created_by": f"user_{i}"} for i in range(num_rows)],
        },
        schema=pa.schema(
            [
                pa.field("datetime", pa.timestamp("us")),
                pa.field("symbol", pa.string()),
                pa.field("bid", pa.float64()),
                pa.field("ask", pa.float64()),
                pa.field(
                    "details", pa.struct([pa.field("created_by", pa.string())])
                ),
            ]
        ),
    )
    iceberg_table = catalog.load_table(f"{namespace}.{table_name}")
    iceberg_table.append(arrow_data)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    ## Enable the failpoint so that tryGetTableImpl returns nullptr for all tables.
    node.query("SYSTEM ENABLE FAILPOINT datalake_try_get_table_return_nullptr")

    try:
        ## This triggers getFilteredTables with engine_column populated (the crash site).
        result = node.query(
            f"SELECT engine FROM system.tables WHERE database = '{CATALOG_NAME}' "
            f"SETTINGS show_data_lake_catalogs_in_system_tables = 1"
        )
        ## With the failpoint, all tables return nullptr so we get empty result.
        assert result.strip() == ""

        ## This triggers the fillData main loop path.
        result = node.query(
            f"SELECT * FROM system.tables WHERE database = '{CATALOG_NAME}' "
            f"SETTINGS show_data_lake_catalogs_in_system_tables = 1"
        )
        assert result.strip() == ""

        ## Also test with count() to exercise a different code path.
        result = node.query(
            f"SELECT count(engine) FROM system.tables WHERE database = '{CATALOG_NAME}' "
            f"AND engine LIKE '%ReplicatedMergeTree' "
            f"SETTINGS show_data_lake_catalogs_in_system_tables = 1"
        )
        assert result.strip() == "0"
    finally:
        node.query(
            "SYSTEM DISABLE FAILPOINT datalake_try_get_table_return_nullptr"
        )

    ## After disabling the failpoint, verify normal operation still works.
    result = node.query(
        f"SELECT count() FROM system.tables WHERE database = '{CATALOG_NAME}' "
        f"AND name ILIKE '%{table_name}%' "
        f"SETTINGS show_data_lake_catalogs_in_system_tables = 1"
    )
    assert int(result.strip()) > 0

    node.query(f"DROP DATABASE IF EXISTS {CATALOG_NAME}")

def test_delete_on_lazy_initialized_table(started_cluster):
    """
    Regression test for https://github.com/ClickHouse/ClickHouse/issues/96806.

    Tables in a DataLakeCatalog database use lazy metadata initialization
    (lazy_init=true), meaning the DataLake metadata is not loaded at table
    construction time.  Prior to the fix, running ALTER TABLE ... DELETE (or
    DELETE FROM ...) as the very first operation on such a table -- before any
    SELECT had a chance to trigger metadata initialization -- resulted in a
    LOGICAL_ERROR: 'Metadata is not initialized'.
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_delete_lazy_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)
    create_clickhouse_iceberg_table(
        started_cluster, node, root_namespace, table_name, "(x String)"
    )

    # Insert rows without any prior SELECT so that metadata starts uninitialized.
    node.query(
        f"INSERT INTO {CATALOG_NAME}.`{root_namespace}.{table_name}` VALUES ('keep');",
        settings={"allow_insert_into_iceberg": 1, "write_full_path_in_iceberg_metadata": 1},
    )
    node.query(
        f"INSERT INTO {CATALOG_NAME}.`{root_namespace}.{table_name}` VALUES ('delete_me');",
        settings={"allow_insert_into_iceberg": 1, "write_full_path_in_iceberg_metadata": 1},
    )

    # Run ALTER TABLE DELETE without a prior SELECT.  This is exactly the query
    # that triggered LOGICAL_ERROR: 'Metadata is not initialized' before the fix.
    node.query(
        f"ALTER TABLE {CATALOG_NAME}.`{root_namespace}.{table_name}` DELETE WHERE x = 'delete_me';",
        settings={"allow_insert_into_iceberg": 1, "write_full_path_in_iceberg_metadata": 1},
    )

    # Also exercise the DELETE FROM syntax (InterpreterDeleteQuery path).
    node.query(
        f"DELETE FROM {CATALOG_NAME}.`{root_namespace}.{table_name}` WHERE x = 'keep';",
        settings={"allow_insert_into_iceberg": 1, "write_full_path_in_iceberg_metadata": 1},
    )

    assert node.query(f"SELECT count() FROM {CATALOG_NAME}.`{root_namespace}.{table_name}`") == "0\n"


def test_writes_schema_evolution(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_writes_schema_evolution_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"
    table_ref = f"{CATALOG_NAME}.`{root_namespace}.{table_name}`"
    write_settings = {"allow_insert_into_iceberg": 1, "write_full_path_in_iceberg_metadata": 1}

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)
    create_clickhouse_iceberg_table(started_cluster, node, root_namespace, table_name, "(x String, y Int32)")

    node.query(f"INSERT INTO {table_ref} VALUES ('123', 1);", settings=write_settings)

    node.query(f"ALTER TABLE {table_ref} ADD COLUMN z Nullable(String);", settings=write_settings)
    assert "z" in node.query(f"DESCRIBE TABLE {table_ref}", settings=write_settings)
    assert node.query(f"SELECT x, y, z FROM {table_ref} ORDER BY ALL", settings=write_settings) == "123\t1\t\\N\n"

    node.query(f"INSERT INTO {table_ref} VALUES ('456', 2, 'hello');", settings=write_settings)
    assert (
        node.query(f"SELECT x, y, z FROM {table_ref} ORDER BY ALL", settings=write_settings)
        == "123\t1\t\\N\n456\t2\thello\n"
    )

    node.query(f"ALTER TABLE {table_ref} RENAME COLUMN z TO w;", settings=write_settings)
    assert "w" in node.query(f"DESCRIBE TABLE {table_ref}", settings=write_settings)
    assert (
        node.query(f"SELECT x, y, w FROM {table_ref} ORDER BY ALL", settings=write_settings)
        == "123\t1\t\\N\n456\t2\thello\n"
    )


def test_writes_schema_evolution_concurrent_add_columns(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_writes_schema_evolution_concurrent_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"
    table_ref = f"{CATALOG_NAME}.`{root_namespace}.{table_name}`"
    write_settings = {"allow_insert_into_iceberg": 1, "write_full_path_in_iceberg_metadata": 1}

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)
    create_clickhouse_iceberg_table(started_cluster, node, root_namespace, table_name, "(x String, y Int32)")

    node.query(f"INSERT INTO {table_ref} VALUES ('123', 1);", settings=write_settings)

    # Concurrent ADD COLUMN commits must contend on the REST catalog to surface
    # the commit-conflict/retry race. A handful of concurrent writers is enough
    # to interleave; the original count of 10 just multiplied catalog round-trips.
    num_columns = 4

    def add_column(idx):
        node.query(
            f"ALTER TABLE {table_ref} ADD COLUMN col_{idx} Nullable(String);",
            settings=write_settings,
        )

    with ThreadPoolExecutor(max_workers=num_columns) as executor:
        list(executor.map(add_column, range(num_columns)))

    description = node.query(f"DESCRIBE TABLE {table_ref}", settings=write_settings)
    for idx in range(num_columns):
        assert f"col_{idx}" in description, f"col_{idx} missing from:\n{description}"

    columns = [line.split("\t")[0] for line in description.strip().split("\n")]
    assert sorted(columns) == sorted(["x", "y"] + [f"col_{idx}" for idx in range(num_columns)])

    select_cols = ", ".join(["x", "y"] + [f"col_{idx}" for idx in range(num_columns)])
    expected = "123\t1" + "\t\\N" * num_columns + "\n"
    assert node.query(f"SELECT {select_cols} FROM {table_ref} ORDER BY ALL", settings=write_settings) == expected


def test_gcs(started_cluster):
    node = started_cluster.instances["node1"]

    node.query("SYSTEM ENABLE FAILPOINT database_iceberg_gcs")
    node.query(f"DROP DATABASE IF EXISTS {CATALOG_NAME};")

    with pytest.raises(Exception) as err:
        node.query(
            f"""
            CREATE DATABASE {CATALOG_NAME}
            ENGINE = DataLakeCatalog('{BASE_URL}', 'gcs', 'dummy')
            SETTINGS
                catalog_type = 'rest',
                warehouse = 'demo',
            """,
            settings={"allow_database_iceberg": 1},
        )
        assert "Google cloud storage converts to S3" in str(err.value)


def test_invalid_auth_header_format(started_cluster):
    node = started_cluster.instances["node1"]

    node.query(f"DROP DATABASE IF EXISTS {CATALOG_NAME};")
    with pytest.raises(Exception) as err:
        node.query(
            f"""
            SET allow_database_iceberg = 1;
            CREATE DATABASE {CATALOG_NAME}
            ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', 'dummy')
            SETTINGS
                catalog_type = 'rest',
                warehouse = 'demo',
                auth_header = 'wrong.header'
            """
        )
    assert "Invalid auth header format" in str(err.value)


def test_writes_mutate_update(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_writes_mutate_update_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"
    table_ref = f"{CATALOG_NAME}.`{root_namespace}.{table_name}`"
    write_settings = {"allow_insert_into_iceberg": 1, "write_full_path_in_iceberg_metadata": 1}

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)
    create_clickhouse_iceberg_table(started_cluster, node, root_namespace, table_name, "(x String, y Int32)")

    node.query(f"INSERT INTO {table_ref} VALUES ('123', 1);", settings=write_settings)
    node.query(f"INSERT INTO {table_ref} VALUES ('456', 2);", settings=write_settings)
    node.query(f"INSERT INTO {table_ref} VALUES ('999', 3);", settings=write_settings)
    assert node.query(f"SELECT * FROM {table_ref} ORDER BY ALL") == "123\t1\n456\t2\n999\t3\n"

    node.query(f"ALTER TABLE {table_ref} UPDATE x = '777' WHERE x = '123';", settings=write_settings)
    assert node.query(f"SELECT * FROM {table_ref} ORDER BY ALL") == "456\t2\n777\t1\n999\t3\n"

    node.query(f"ALTER TABLE {table_ref} UPDATE x = 'goshan dr' WHERE x = '777';", settings=write_settings)
    assert node.query(f"SELECT * FROM {table_ref} ORDER BY ALL") == "456\t2\n999\t3\ngoshan dr\t1\n"

    node.query(f"ALTER TABLE {table_ref} UPDATE x = 'pudge1000-7' WHERE y = 2;", settings=write_settings)
    assert node.query(f"SELECT * FROM {table_ref} ORDER BY ALL") == "999\t3\ngoshan dr\t1\npudge1000-7\t2\n"


def test_writes_mutate_delete(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_writes_mutate_delete_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"
    table_ref = f"{CATALOG_NAME}.`{root_namespace}.{table_name}`"
    write_settings = {"allow_insert_into_iceberg": 1, "write_full_path_in_iceberg_metadata": 1}

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)
    create_clickhouse_iceberg_table(started_cluster, node, root_namespace, table_name, "(x String)")

    # DELETE on empty table is a no-op.
    node.query(f"ALTER TABLE {table_ref} DELETE WHERE x = 'pudge1000-7';", settings=write_settings)
    assert node.query(f"SELECT * FROM {table_ref} ORDER BY ALL") == ""

    node.query(f"INSERT INTO {table_ref} VALUES ('123');", settings=write_settings)
    node.query(f"INSERT INTO {table_ref} VALUES ('456');", settings=write_settings)
    node.query(f"INSERT INTO {table_ref} VALUES ('789'), ('890'), ('999');", settings=write_settings)
    assert node.query(f"SELECT * FROM {table_ref} ORDER BY ALL") == "123\n456\n789\n890\n999\n"

    # No-match DELETE keeps the table intact.
    node.query(f"ALTER TABLE {table_ref} DELETE WHERE x = 'pudge1000-7';", settings=write_settings)
    assert node.query(f"SELECT * FROM {table_ref} ORDER BY ALL") == "123\n456\n789\n890\n999\n"

    node.query(f"ALTER TABLE {table_ref} DELETE WHERE x = '789';", settings=write_settings)
    assert node.query(f"SELECT * FROM {table_ref} ORDER BY ALL") == "123\n456\n890\n999\n"

    # Lightweight DELETE syntax should work identically against catalog tables.
    node.query(f"DELETE FROM {table_ref} WHERE x = '123';", settings=write_settings)
    assert node.query(f"SELECT * FROM {table_ref} ORDER BY ALL") == "456\n890\n999\n"

    node.query(f"ALTER TABLE {table_ref} DELETE WHERE x = '999';", settings=write_settings)
    assert node.query(f"SELECT * FROM {table_ref} ORDER BY ALL") == "456\n890\n"


def test_iceberg_file_progress_callback(started_cluster):
    """
    Regression test for the `IcebergIterator::next` file-progress callback wiring (PR #105413).

    `IcebergIterator` stored a `FileProgressCallback` but never invoked it, so the
    per-query `Progress.total_bytes_to_read` stayed at zero for Iceberg scans and
    the progress bar showed no estimate. The fix invokes the callback with the data
    file size for every object info returned. The assertion below uses the
    `FileProgressCallbackInvocations` ProfileEvent, which is incremented inside the
    callback lambda installed by `TCPHandler::setFileProgressCallback`, so removing
    the `callback(...)` call in `IcebergIterator::next` makes this event stay at
    zero for the test query.
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_progress_callback_{uuid.uuid4().hex[:8]}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(root_namespace)

    table = create_table(
        catalog,
        root_namespace,
        table_name,
        DEFAULT_SCHEMA,
        PartitionSpec(),
        DEFAULT_SORT_ORDER,
    )

    # Append a small but non-empty batch so the iterator returns a data-file entry.
    num_rows = 50
    data = [generate_record() for _ in range(num_rows)]
    df = pa.Table.from_pylist(data)
    table.append(df)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    # `node.query` uses native TCP, the only protocol path where
    # `setFileProgressCallback` is currently wired. `SELECT *` with `FORMAT Null`
    # forces a full scan: the metadata-only `SELECT count()` path resolves the row
    # count from manifest statistics and bypasses the data-file iterator.
    query_id = f"iceberg_progress_callback_{uuid.uuid4().hex}"
    node.query(
        f"SELECT * FROM {CATALOG_NAME}.`{root_namespace}.{table_name}` FORMAT Null",
        query_id=query_id,
    )

    node.query("SYSTEM FLUSH LOGS")

    # `FileProgressCallbackInvocations` is incremented inside the lambda installed
    # by `TCPHandler::setFileProgressCallback`. For an Iceberg-table scan it can
    # only fire from `IcebergIterator::next` (the generic
    # `StorageObjectStorageSource::KeysIterator` path is replaced by
    # `IcebergIterator` for Iceberg storage), so a non-zero value proves the
    # iterator's `callback(FileProgress(...))` invocation was executed.
    profile_event_value = node.query(
        f"""
        SELECT ProfileEvents['FileProgressCallbackInvocations']
        FROM system.query_log
        WHERE query_id = '{query_id}' AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
        """
    ).strip()
    assert profile_event_value, (
        f"`system.query_log` has no `QueryFinish` row for query_id={query_id}."
    )
    file_progress_callback_invocations = int(profile_event_value)
    assert file_progress_callback_invocations > 0, (
        f"Expected `FileProgressCallbackInvocations` > 0 from the Iceberg scan, "
        f"got {file_progress_callback_invocations}. "
        f"`IcebergIterator::next` did not invoke the file-progress callback "
        f"(regression of PR #105413 wiring)."
    )


def test_database_priority_over_namespace(started_cluster):
    """
    Test that database.table interpretation takes priority over namespace.table.
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_priority_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    table_name = "priority_table"

    catalog = load_catalog_impl(started_cluster)

    catalog.create_namespace(namespace)
    iceberg_table = create_table(catalog, namespace, table_name)

    data = [generate_record() for _ in range(5)]
    df = pa.Table.from_pylist(data)
    iceberg_table.append(df)

    # create the DataLakeCatalog database
    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    # create a regular database with the same name as the namespace
    node.query(f"DROP DATABASE IF EXISTS `{namespace}`")
    node.query(f"CREATE DATABASE `{namespace}`")
    
    # create a table with the same name in the regular database (3 rows)
    node.query(f"CREATE TABLE `{namespace}`.{table_name} (id UInt64) ENGINE = Memory")
    node.query(f"INSERT INTO `{namespace}`.{table_name} VALUES (1), (2), (3)")

    # When in the DataLakeCatalog database, querying namespace.table should resolve
    # to the regular database (db.table takes priority)
    count = int(node.query(f"USE {CATALOG_NAME}; SELECT count() FROM {namespace}.{table_name}"))
    assert count == 3, f"Expected 3 rows from regular database, got {count}"

    # To access the iceberg table, use backticks
    count_iceberg = int(node.query(f"USE {CATALOG_NAME}; SELECT count() FROM `{namespace}.{table_name}`"))
    assert count_iceberg == 5, f"Expected 5 rows from iceberg table, got {count_iceberg}"

    node.query(f"DROP DATABASE IF EXISTS `{namespace}`")


def test_use_database_with_namespace(started_cluster):
    """
    Test USE db.namespace syntax for DataLakeCatalog databases
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_use_ns_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    table_name = "use_test_table"

    catalog = load_catalog_impl(started_cluster)

    catalog.create_namespace(namespace)
    iceberg_table = create_table(catalog, namespace, table_name)

    data = [generate_record() for _ in range(5)]
    df = pa.Table.from_pylist(data)
    iceberg_table.append(df)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    # Test USE db.namespace syntax - after this, short table names get namespace prefix
    count = int(node.query(f"USE {CATALOG_NAME}.{namespace}; SELECT count() FROM {table_name}"))
    assert count == 5, f"Expected 5 rows after USE db.namespace, got {count}"

    # Verify we can also use the full path
    count_full = int(node.query(f"SELECT count() FROM {CATALOG_NAME}.{namespace}.{table_name}"))
    assert count_full == 5, f"Expected 5 rows with full path, got {count_full}"

    # check that prefix is cleared when switching to regular db
    _, error = node.query_and_get_answer_with_error(f"USE {CATALOG_NAME}.{namespace}; USE default; SELECT 1 FROM {table_name}")
    assert "UNKNOWN_TABLE" in error or "doesn't exist" in error, f"Expected UNKNOWN_TABLE error, got: {error}"

    # Test USE catalog (without prefix) and then query with namespace.table
    count_ns = int(node.query(f"USE {CATALOG_NAME}; SELECT count() FROM {namespace}.{table_name}"))
    assert count_ns == 5, f"Expected 5 rows with namespace.table after USE catalog, got {count_ns}"


def test_three_part_identifier(started_cluster):
    """
    Test 3-part compound identifier syntax: db.namespace.table
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_three_part_identifier_{uuid.uuid4().hex[:8]}"
    table_name = f"{test_ref}_table"
    namespace = f"{test_ref}_ns"  # Single-level namespace

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)

    table = create_table(catalog, namespace, table_name)

    num_rows = 5
    data = [generate_record() for _ in range(num_rows)]
    df = pa.Table.from_pylist(data)
    table.append(df)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    # This should work the same as demo.`namespace.table`
    count_3part = int(node.query(f"SELECT count() FROM {CATALOG_NAME}.{namespace}.{table_name}"))
    assert count_3part == num_rows, f"Expected {num_rows} rows, got {count_3part}"

    # compare with backtick syntax - should give same result
    count_backtick = int(node.query(f"SELECT count() FROM {CATALOG_NAME}.`{namespace}.{table_name}`"))
    assert count_3part == count_backtick, "3-part and backtick syntax should return same results"

    # EXISTS TABLE with 3-part identifier
    exists_result = node.query(f"EXISTS TABLE {CATALOG_NAME}.{namespace}.{table_name}").strip()
    assert exists_result == "1", f"EXISTS TABLE should return 1, got {exists_result}"

    # DESCRIBE with 3-part identifier
    desc_3part = node.query(f"DESCRIBE {CATALOG_NAME}.{namespace}.{table_name}")
    desc_backtick = node.query(f"DESCRIBE {CATALOG_NAME}.`{namespace}.{table_name}`")
    assert desc_3part == desc_backtick, "DESCRIBE output should match between syntaxes"

    # SHOW CREATE TABLE with 3-part identifier
    show_create_3part = node.query(f"SHOW CREATE TABLE {CATALOG_NAME}.{namespace}.{table_name}")
    show_create_backtick = node.query(f"SHOW CREATE TABLE {CATALOG_NAME}.`{namespace}.{table_name}`")
    assert show_create_3part == show_create_backtick, "SHOW CREATE TABLE output should match between syntaxes"

    # non-existent table with 3-part identifier
    try:
        node.query(f"SELECT * FROM {CATALOG_NAME}.{namespace}.nonexistent_table")
        assert False, "Should have raised exception for non-existent table"
    except Exception as e:
        assert "doesn't exist" in str(e) or "UNKNOWN_TABLE" in str(e)


def test_multi_level_namespace(started_cluster):
    """
    Test N-part compound identifier syntax with multiple namespace levels: db.ns1.ns2.table
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_multi_ns_{uuid.uuid4().hex[:8]}"
    table_name = f"{test_ref}_table"
    ns_level1 = f"{test_ref}_l1"
    ns_level2 = f"{test_ref}_l2"
    multi_namespace = f"{ns_level1}.{ns_level2}"  # Two-level namespace

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(multi_namespace)

    table = create_table(catalog, multi_namespace, table_name)

    num_rows = 5
    data = [generate_record() for _ in range(num_rows)]
    df = pa.Table.from_pylist(data)
    table.append(df)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    # 4-part identifier SELECT (db.ns1.ns2.table)
    count_4part = int(node.query(f"SELECT count() FROM {CATALOG_NAME}.{ns_level1}.{ns_level2}.{table_name}"))
    assert count_4part == num_rows, f"Expected {num_rows} rows, got {count_4part}"

    # compare with backtick syntax - should give same result
    count_backtick = int(node.query(f"SELECT count() FROM {CATALOG_NAME}.`{multi_namespace}.{table_name}`"))
    assert count_4part == count_backtick, "4-part and backtick syntax should return same results"

    # EXISTS TABLE with 4-part identifier
    exists_result = node.query(f"EXISTS TABLE {CATALOG_NAME}.{ns_level1}.{ns_level2}.{table_name}").strip()
    assert exists_result == "1", f"EXISTS TABLE should return 1, got {exists_result}"

    # DESCRIBE with 4-part identifier
    desc_4part = node.query(f"DESCRIBE {CATALOG_NAME}.{ns_level1}.{ns_level2}.{table_name}")
    desc_backtick = node.query(f"DESCRIBE {CATALOG_NAME}.`{multi_namespace}.{table_name}`")
    assert desc_4part == desc_backtick, "DESCRIBE output should match between syntaxes"

    # SHOW CREATE TABLE with 4-part identifier
    show_create_4part = node.query(f"SHOW CREATE TABLE {CATALOG_NAME}.{ns_level1}.{ns_level2}.{table_name}")
    show_create_backtick = node.query(f"SHOW CREATE TABLE {CATALOG_NAME}.`{multi_namespace}.{table_name}`")
    assert show_create_4part == show_create_backtick, "SHOW CREATE TABLE output should match between syntaxes"

    # non-existent table with 4-part identifier
    try:
        node.query(f"SELECT * FROM {CATALOG_NAME}.{ns_level1}.{ns_level2}.nonexistent_table")
        assert False, "Should have raised exception for non-existent table"
    except Exception as e:
        assert "doesn't exist" in str(e) or "UNKNOWN_TABLE" in str(e)


def test_namespace_prefix_in_non_select_queries(started_cluster):
    """
    Bare table names under USE db.namespace must work beyond SELECT:
    EXISTS/DESCRIBE/SHOW CREATE resolve through Context::resolveStorageID.
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_ns_resolve_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    table_name = "resolve_test_table"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)
    iceberg_table = create_table(catalog, namespace, table_name)

    data = [generate_record() for _ in range(3)]
    iceberg_table.append(pa.Table.from_pylist(data))

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    use = f"USE {CATALOG_NAME}.{namespace}; "
    assert node.query(use + f"EXISTS TABLE {table_name}").strip() == "1"

    describe = node.query(use + f"DESCRIBE TABLE {table_name}")
    assert "id" in describe and "data" in describe, f"DESCRIBE failed: {describe}"

    show_create = node.query(use + f"SHOW CREATE TABLE {table_name}")
    assert f"`{namespace}.{table_name}`" in show_create, f"SHOW CREATE failed: {show_create}"


def test_namespace_prefix_query_cache_isolation(started_cluster):
    """
    The same unqualified query under different USE db.namespace prefixes must not
    share query cache entries (the prefix is part of the cache key).
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_ns_qcache_{uuid.uuid4().hex[:8]}"
    ns_1 = f"ns1_{test_ref}"
    ns_2 = f"ns2_{test_ref}"
    table_name = "qcache_test_table"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(ns_1)
    catalog.create_namespace(ns_2)

    table_1 = create_table(catalog, ns_1, table_name)
    table_2 = create_table(catalog, ns_2, table_name)

    table_1.append(pa.Table.from_pylist([generate_record() for _ in range(2)]))
    table_2.append(pa.Table.from_pylist([generate_record() for _ in range(5)]))

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    query = f"SELECT count() FROM {table_name} SETTINGS use_query_cache = 1"
    count_1 = int(node.query(f"USE {CATALOG_NAME}.{ns_1}; {query}"))
    count_2 = int(node.query(f"USE {CATALOG_NAME}.{ns_2}; {query}"))
    assert count_1 == 2, f"Expected 2 rows in {ns_1}, got {count_1}"
    assert count_2 == 5, f"Expected 5 rows in {ns_2} (cache must not leak across namespaces), got {count_2}"


def test_namespace_prefix_grants(started_cluster):
    """
    GRANT on a bare table name under USE db.namespace must target the
    namespace-qualified table that SELECT resolves to.
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_ns_grant_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    table_name = "grant_test_table"
    user = f"user_{test_ref}"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)
    create_table(catalog, namespace, table_name)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    node.query(f"DROP USER IF EXISTS {user}")
    node.query(f"CREATE USER {user}")
    try:
        node.query(f"USE {CATALOG_NAME}.{namespace}; GRANT SELECT ON {table_name} TO {user}")
        grants = node.query(f"SHOW GRANTS FOR {user}")
        assert (
            f"`{namespace}.{table_name}`" in grants
        ), f"grant is not namespace-qualified: {grants}"

        # A whole-database grant under the prefix scopes to the namespace.
        node.query(f"USE {CATALOG_NAME}.{namespace}; GRANT INSERT ON * TO {user}")
        grants = node.query(f"SHOW GRANTS FOR {user}")
        assert (
            f"`{namespace}.`*" in grants
        ), f"any-table grant is not namespace-scoped: {grants}"
    finally:
        node.query(f"DROP USER IF EXISTS {user}")


def test_namespace_prefix_create_view(started_cluster):
    """
    CREATE VIEW under USE db.namespace must store the SELECT with the
    namespace-qualified table name.
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_ns_view_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    table_name = "view_src_table"
    view = f"default.v_{test_ref}"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)
    iceberg_table = create_table(catalog, namespace, table_name)
    iceberg_table.append(pa.Table.from_pylist([generate_record() for _ in range(4)]))

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    node.query(f"DROP VIEW IF EXISTS {view}")
    try:
        node.query(
            f"USE {CATALOG_NAME}.{namespace}; CREATE VIEW {view} AS SELECT * FROM {table_name}"
        )
        show_create = node.query(f"SHOW CREATE TABLE {view}")
        assert (
            f"`{namespace}.{table_name}`" in show_create
        ), f"view definition is not namespace-qualified: {show_create}"

        count = int(node.query(f"SELECT count() FROM {view}"))
        assert count == 4, f"expected 4 rows through the view, got {count}"
    finally:
        node.query(f"DROP VIEW IF EXISTS {view}")


def test_namespace_prefix_distributed_join(started_cluster):
    """
    A bare table name in the JOIN section of a query shipped to remote servers
    must keep the namespace prefix (remote servers have no session prefix).
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_ns_dist_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    table_name = "dist_join_table"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)
    iceberg_table = create_table(catalog, namespace, table_name)
    iceberg_table.append(pa.Table.from_pylist([generate_record() for _ in range(3)]))

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    # 127.0.0.2 forces the query through the remote path on the same server.
    count = int(
        node.query(
            f"USE {CATALOG_NAME}.{namespace}; "
            f"SELECT count() FROM remote('127.0.0.2', system.one) AS o CROSS JOIN {table_name} AS r"
        )
    )
    assert count == 3, f"expected 3 rows via distributed JOIN, got {count}"


def test_namespace_prefix_row_policies(started_cluster):
    """
    CREATE/SHOW/DROP ROW POLICY on a bare table name under USE db.namespace must
    target the namespace-qualified table, like SELECT does.
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_ns_policy_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    table_name = "policy_test_table"
    policy = f"pol_{test_ref}"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)
    create_table(catalog, namespace, table_name)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    use = f"USE {CATALOG_NAME}.{namespace}; "
    node.query(use + f"CREATE ROW POLICY {policy} ON {table_name} USING 1 TO ALL")
    try:
        show_create = node.query(
            use + f"SHOW CREATE ROW POLICY {policy} ON {table_name}"
        )
        assert (
            f"`{namespace}.{table_name}`" in show_create
        ), f"policy target is not namespace-qualified: {show_create}"

        policies = node.query(use + f"SHOW ROW POLICIES ON {table_name}")
        assert policy in policies, f"policy not listed for the namespaced table: {policies}"
    finally:
        node.query(use + f"DROP ROW POLICY IF EXISTS {policy} ON {table_name}")
    remaining = node.query(f"SHOW ROW POLICIES ON {CATALOG_NAME}.`{namespace}.{table_name}`")
    assert policy not in remaining, f"policy not dropped: {remaining}"


def test_namespace_prefix_show_columns_and_reconnect(started_cluster):
    """
    SHOW COLUMNS/INDEXES must honor the namespace (bare and dotted forms), and a
    client default database "catalog.namespace" must survive a fresh connection.
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_ns_showcols_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    table_name = "showcols_test_table"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)
    iceberg_table = create_table(catalog, namespace, table_name)
    iceberg_table.append(pa.Table.from_pylist([generate_record() for _ in range(2)]))

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    # Bare name under the USE prefix.
    cols = node.query(
        f"USE {CATALOG_NAME}.{namespace}; SHOW COLUMNS FROM {table_name}"
    )
    assert "id" in cols and "data" in cols, f"SHOW COLUMNS lost the namespace: {cols}"

    # Dotted form without USE.
    cols_dotted = node.query(f"SHOW COLUMNS FROM {CATALOG_NAME}.{namespace}.{table_name}")
    assert "id" in cols_dotted and "data" in cols_dotted, f"dotted SHOW COLUMNS failed: {cols_dotted}"

    # A fresh connection with default database "catalog.namespace" (as persisted by
    # clients after USE) must resolve bare names in the namespace.
    count = int(
        node.query(
            f"SELECT count() FROM {table_name}",
            database=f"{CATALOG_NAME}.{namespace}",
        )
    )
    assert count == 2, f"default-database handshake lost the namespace: {count}"


def test_namespace_two_part_in_non_select_queries(started_cluster):
    """
    Under USE catalog (no namespace), `namespace.table` must resolve in the shared
    storage resolver too: EXISTS/DESCRIBE/SHOW CREATE/INSERT, not just SELECT.
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_ns_twopart_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    table_name = "twopart_test_table"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)
    iceberg_table = create_table(catalog, namespace, table_name)
    iceberg_table.append(pa.Table.from_pylist([generate_record() for _ in range(2)]))

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    use = f"USE {CATALOG_NAME}; "
    assert node.query(use + f"EXISTS TABLE {namespace}.{table_name}").strip() == "1"

    describe = node.query(use + f"DESCRIBE TABLE {namespace}.{table_name}")
    assert "id" in describe and "data" in describe, f"DESCRIBE failed: {describe}"

    show_create = node.query(use + f"SHOW CREATE TABLE {namespace}.{table_name}")
    assert f"`{namespace}.{table_name}`" in show_create, f"SHOW CREATE failed: {show_create}"

    count = int(node.query(use + f"SELECT count() FROM {namespace}.{table_name}"))
    assert count == 2

    cols = node.query(use + f"SHOW COLUMNS FROM {namespace}.{table_name}")
    assert "id" in cols and "data" in cols, f"two-part SHOW COLUMNS failed: {cols}"

    # Iceberg tables expose no data-skipping indices; success without error is enough.
    node.query(use + f"SHOW INDEXES FROM {namespace}.{table_name}")


def test_namespace_prefix_materialized_view_target(started_cluster):
    """
    An unqualified TO target of a materialized view under USE db.namespace must be
    stored namespace-qualified.
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_ns_mvtarget_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    table_name = "mv_target_table"
    mv = f"default.mv_{test_ref}"
    src_table = f"default.src_{test_ref}"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)
    create_table(catalog, namespace, table_name)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    node.query(f"CREATE TABLE {src_table} (id Float64, data String) ENGINE = Memory")
    try:
        node.query(
            f"USE {CATALOG_NAME}.{namespace}; "
            f"CREATE MATERIALIZED VIEW {mv} TO {table_name} AS SELECT id, data FROM {src_table}"
        )
        show_create = node.query(f"SHOW CREATE TABLE {mv}")
        assert (
            f"`{namespace}.{table_name}`" in show_create
        ), f"MV target is not namespace-qualified: {show_create}"
    finally:
        node.query(f"DROP VIEW IF EXISTS {mv}")
        node.query(f"DROP TABLE IF EXISTS {src_table}")


def test_namespace_prefix_mysql_field_list(started_cluster):
    """
    COM_FIELD_LIST over the MySQL protocol must honor the namespace selected by a
    default database of the form "catalog.namespace".
    """
    import pymysql

    node = started_cluster.instances["node1"]

    test_ref = f"test_ns_fieldlist_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    table_name = "fieldlist_test_table"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)
    create_table(catalog, namespace, table_name)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    conn = pymysql.connect(
        host=started_cluster.get_instance_ip("node1"),
        port=9004,
        user="default",
        password="",
        database=f"{CATALOG_NAME}.{namespace}",
    )
    try:
        # COM_FIELD_LIST (0x04): table name, NUL, wildcard.
        conn._execute_command(4, table_name.encode() + b"\x00")
        columns = []
        while True:
            packet = conn._read_packet()
            if packet.is_eof_packet():
                break
            # Column definition: catalog (lenenc "def"), schema, table, org_table, name, ...
            data = packet.get_all_data()
            columns.append(data)
        assert len(columns) >= 2, f"expected column definitions, got {len(columns)} packets"
        joined = b"".join(columns)
        assert b"id" in joined and b"data" in joined, f"unexpected field list: {joined[:200]!r}"
    finally:
        conn.close()


def test_namespace_prefix_create_drop_table(started_cluster):
    """
    CREATE TABLE with a bare name under USE db.namespace must create the table in
    the namespace; DROP TABLE must resolve both bare and namespace.table forms.
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_ns_createdrop_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    table_name = "create_drop_table"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    node.query(
        f"USE {CATALOG_NAME}.{namespace}; "
        f"CREATE TABLE {table_name} (x String) "
        f"ENGINE = IcebergS3('http://minio1:9001/warehouse-rest/{table_name}/', '{minio_access_key}', '{minio_secret_key}')",
        settings={"write_full_path_in_iceberg_metadata": 1},
    )
    full_name = f"{CATALOG_NAME}.`{namespace}.{table_name}`"
    assert node.query(f"EXISTS TABLE {full_name}").strip() == "1"

    # DROP via the two-part form under USE catalog.
    node.query(f"USE {CATALOG_NAME}; DROP TABLE {namespace}.{table_name}")
    assert node.query(f"EXISTS TABLE {full_name}").strip() == "0"


def test_namespace_prefix_update_authorization(started_cluster):
    """
    UPDATE under USE db.namespace must authorize the namespace-qualified table,
    not the bare name.
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_ns_updauth_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    table_name = "upd_auth_table"
    user = f"user_{test_ref}"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)
    create_table(catalog, namespace, table_name)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    node.query(f"DROP USER IF EXISTS {user}")
    node.query(f"CREATE USER {user}")
    try:
        update = (
            f"USE {CATALOG_NAME}.{namespace}; "
            f"UPDATE {table_name} SET data = 'x' WHERE 1 "
            f"SETTINGS enable_lightweight_update = 1"
        )
        # Grant on the bare (wrong) name only: must be denied.
        node.query(f"GRANT SELECT, ALTER UPDATE ON {CATALOG_NAME}.{table_name} TO {user}")
        _, err = node.query_and_get_answer_with_error(update, user=user)
        assert "ACCESS_DENIED" in err, f"expected ACCESS_DENIED with bare-name grant, got: {err}"

        # Grant on the namespace-qualified table: authorization must pass
        # (the engine then rejects lightweight updates, which is fine).
        node.query(
            f"GRANT SELECT, ALTER UPDATE ON {CATALOG_NAME}.`{namespace}.{table_name}` TO {user}"
        )
        _, err = node.query_and_get_answer_with_error(update, user=user)
        assert "ACCESS_DENIED" not in err, f"unexpected ACCESS_DENIED with folded-name grant: {err}"
    finally:
        node.query(f"DROP USER IF EXISTS {user}")


def test_namespace_prefix_row_policy_any_table_rejected(started_cluster):
    """
    CREATE ROW POLICY ON * under USE db.namespace would silently target the whole
    catalog, so it must be rejected.
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_ns_polstar_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    policy = f"pol_{test_ref}"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    _, err = node.query_and_get_answer_with_error(
        f"USE {CATALOG_NAME}.{namespace}; CREATE ROW POLICY {policy} ON * USING 1 TO ALL"
    )
    assert "BAD_ARGUMENTS" in err or "not supported while a namespace" in err, (
        f"expected rejection of ON * under a namespace, got: {err}"
    )
    node.query(f"DROP ROW POLICY IF EXISTS {policy} ON {CATALOG_NAME}.*")


def test_namespace_prefix_create_authorization(started_cluster):
    """
    CREATE TABLE under USE db.namespace must authorize the namespace-qualified
    name, not the bare one.
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_ns_createauth_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    table_name = "create_auth_table"
    user = f"user_{test_ref}"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    create = (
        f"USE {CATALOG_NAME}.{namespace}; "
        f"CREATE TABLE {table_name} (x String) "
        f"ENGINE = IcebergS3('http://minio1:9001/warehouse-rest/{table_name}/', '{minio_access_key}', '{minio_secret_key}') "
        f"SETTINGS write_full_path_in_iceberg_metadata = 1"
    )
    node.query(f"DROP USER IF EXISTS {user}")
    node.query(f"CREATE USER {user}")
    try:
        node.query(f"GRANT SHOW DATABASES ON *.* TO {user}")
        node.query(f"GRANT S3 ON *.* TO {user}")
        # Grant on the bare (wrong) name only: must be denied.
        node.query(f"GRANT CREATE TABLE ON {CATALOG_NAME}.{table_name} TO {user}")
        _, err = node.query_and_get_answer_with_error(create, user=user)
        assert "ACCESS_DENIED" in err, f"expected ACCESS_DENIED with bare-name grant, got: {err}"

        # Grant on the namespace-qualified name: creation must be authorized.
        node.query(f"GRANT CREATE TABLE ON {CATALOG_NAME}.`{namespace}.{table_name}` TO {user}")
        node.query(create, user=user)
        full_name = f"{CATALOG_NAME}.`{namespace}.{table_name}`"
        assert node.query(f"EXISTS TABLE {full_name}").strip() == "1"
        node.query(f"DROP TABLE {full_name}")
    finally:
        node.query(f"DROP USER IF EXISTS {user}")


def test_namespace_prefix_create_as(started_cluster):
    """
    CREATE TABLE ... AS <source> must resolve the source through the namespace-aware
    resolver: bare names under USE db.namespace, namespace.table under USE catalog,
    and the full three-part form.
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_ns_createas_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    table_name = "create_as_src"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)
    create_table(catalog, namespace, table_name)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    copies = [f"default.copy_{i}_{test_ref}" for i in range(3)]
    try:
        node.query(
            f"USE {CATALOG_NAME}.{namespace}; "
            f"CREATE TABLE {copies[0]} ENGINE = Memory AS {table_name}"
        )
        node.query(
            f"USE {CATALOG_NAME}; "
            f"CREATE TABLE {copies[1]} ENGINE = Memory AS {namespace}.{table_name}"
        )
        node.query(
            f"CREATE TABLE {copies[2]} ENGINE = Memory AS {CATALOG_NAME}.{namespace}.{table_name}"
        )
        for copy in copies:
            describe = node.query(f"DESCRIBE TABLE {copy}")
            assert "id" in describe and "data" in describe, f"{copy} structure wrong: {describe}"
    finally:
        for copy in copies:
            node.query(f"DROP TABLE IF EXISTS {copy}")


def test_namespace_prefix_optimize_and_partition_authorization(started_cluster):
    """
    OPTIMIZE and ALTER ... PARTITION sub-table references under USE db.namespace
    must authorize the namespace-qualified names.
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_ns_optauth_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    table_name = "opt_auth_table"
    src_name = "opt_auth_src"
    user = f"user_{test_ref}"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)
    create_table(catalog, namespace, table_name)
    create_table(catalog, namespace, src_name)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    node.query(f"DROP USER IF EXISTS {user}")
    node.query(f"CREATE USER {user}")
    try:
        optimize = f"USE {CATALOG_NAME}.{namespace}; OPTIMIZE TABLE {table_name}"
        replace = (
            f"USE {CATALOG_NAME}.{namespace}; "
            f"ALTER TABLE {table_name} REPLACE PARTITION tuple() FROM {src_name}"
        )

        # Grants on the bare (wrong) names only: must be denied.
        node.query(
            f"GRANT SELECT, INSERT, ALTER, OPTIMIZE ON {CATALOG_NAME}.{table_name} TO {user}"
        )
        node.query(
            f"GRANT SELECT, INSERT, ALTER, OPTIMIZE ON {CATALOG_NAME}.{src_name} TO {user}"
        )
        _, err = node.query_and_get_answer_with_error(optimize, user=user)
        assert "ACCESS_DENIED" in err, f"OPTIMIZE: expected ACCESS_DENIED, got: {err}"
        _, err = node.query_and_get_answer_with_error(replace, user=user)
        assert "ACCESS_DENIED" in err, f"REPLACE PARTITION: expected ACCESS_DENIED, got: {err}"

        # Namespace-scoped wildcard grant: authorization must pass (the engine may
        # still reject the operation itself, which is fine).
        node.query(
            f"GRANT SELECT, INSERT, ALTER, OPTIMIZE ON {CATALOG_NAME}.`{namespace}.`* TO {user}"
        )
        _, err = node.query_and_get_answer_with_error(optimize, user=user)
        assert "ACCESS_DENIED" not in err, f"OPTIMIZE: unexpected ACCESS_DENIED: {err}"
        _, err = node.query_and_get_answer_with_error(replace, user=user)
        assert "ACCESS_DENIED" not in err, f"REPLACE PARTITION: unexpected ACCESS_DENIED: {err}"
    finally:
        node.query(f"DROP USER IF EXISTS {user}")


def test_namespace_prefix_mutation_expression(started_cluster):
    """
    Table references inside ALTER ... UPDATE expressions under USE db.namespace
    must resolve inside the namespace, like any other table reference.
    """
    node = started_cluster.instances["node1"]

    test_ref = f"test_ns_mutexpr_{uuid.uuid4().hex[:8]}"
    namespace = f"ns_{test_ref}"
    target = "mut_expr_target"
    src = "mut_expr_src"
    write_settings = {"allow_insert_into_iceberg": 1, "write_full_path_in_iceberg_metadata": 1}

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)

    create_clickhouse_iceberg_table(started_cluster, node, namespace, target, "(x String, y Int32)")
    create_clickhouse_iceberg_table(started_cluster, node, namespace, src, "(x String, y Int32)")

    node.query(
        f"INSERT INTO {CATALOG_NAME}.`{namespace}.{target}` VALUES ('old', 1)",
        settings=write_settings,
    )
    node.query(
        f"INSERT INTO {CATALOG_NAME}.`{namespace}.{src}` VALUES ('fresh', 2)",
        settings=write_settings,
    )

    node.query(
        f"USE {CATALOG_NAME}.{namespace}; "
        f"ALTER TABLE {target} UPDATE x = (SELECT any(x) FROM {src}) WHERE 1",
        settings=write_settings,
    )
    result = node.query(f"SELECT x FROM {CATALOG_NAME}.`{namespace}.{target}`").strip()
    assert result == "fresh", f"mutation expression resolved the wrong source: {result}"
