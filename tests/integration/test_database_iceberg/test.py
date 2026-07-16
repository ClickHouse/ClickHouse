import io
import json
import logging
import random
import re
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
from helpers.s3_tools import get_file_contents

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
                "configs/display_secrets.xml",
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


def escape_like_literal(s):
    # Escape SQL LIKE wildcards (`%`, `_`) and `\` so the value matches literally
    # (ClickHouse keeps the backslash, so one backslash in the query text suffices).
    return re.sub(r"([\\%_])", r"\\\1", s)


def test_namespace_filter_pushdown(started_cluster):
    """
    Verify that `system.tables` predicates that fully bind the namespace
    (`name = '<ns>.<table>'`, `name LIKE '<ns>.%'`) only fetch the table list
    from the targeted namespace instead of enumerating the whole catalog.
    See issue #105022.

    Checking the result rows alone is not enough: an implementation that lists
    the whole catalog and filters in memory would return the same rows. To prove
    the scoped catalog API is actually used we also count the per-namespace
    `Received tables response for namespace: <ns>` log line that `RestCatalog`
    emits for every namespace whose `.../tables` endpoint it hits. A scoped query
    must bump the count for the targeted namespace while leaving the sibling
    namespace untouched; a regression to a full-catalog scan would also fetch the
    sibling and fail the assertion.
    """
    node = started_cluster.instances["node1"]

    root_namespace = f"clickhouse_{uuid.uuid4()}"
    namespace_1 = f"{root_namespace}.target.scope"
    namespace_2 = f"{root_namespace}.other.scope"
    namespace_1_tables = ["scoped_a", "scoped_b"]
    namespace_2_tables = ["other_a", "other_b"]

    catalog = load_catalog_impl(started_cluster)

    for namespace in [namespace_1, namespace_2]:
        catalog.create_namespace(namespace)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    for table in namespace_1_tables:
        create_table(catalog, namespace_1, table)
    for table in namespace_2_tables:
        create_table(catalog, namespace_2, table)

    def namespace_listings(namespace):
        # Number of times RestCatalog has fetched the table list of `namespace`
        # so far. `count_in_log` only scans the current (non-rotated) log file,
        # which is what we want for before/after deltas within a single test.
        return int(
            node.count_in_log(f"Received tables response for namespace: {namespace}")
        )

    def assert_scoped(query, expected):
        # Run a query that should be scoped to `namespace_1` and assert both the
        # result rows and that only the target namespace's table list was fetched.
        before_target = namespace_listings(namespace_1)
        before_sibling = namespace_listings(namespace_2)

        assert expected == node.query(query).strip()

        # The catalog requests run on a background thread pool, so the log line
        # may land slightly after the query returns. Wait for the target listing
        # to confirm the query really reached the catalog before checking that the
        # sibling was left alone.
        for _ in range(30):
            if namespace_listings(namespace_1) > before_target:
                break
            time.sleep(0.5)
        else:
            raise AssertionError(
                f"Scoped query did not fetch the table list of '{namespace_1}': {query}"
            )

        assert namespace_listings(namespace_2) == before_sibling, (
            f"Scoped query for '{namespace_1}' also fetched the sibling namespace "
            f"'{namespace_2}' — namespace push-down regressed to a full-catalog "
            f"scan: {query}"
        )

    expected_ns1 = "\n".join(sorted(f"{namespace_1}.{t}" for t in namespace_1_tables))

    # Case-sensitive LIKE pushdown. The namespace's literal `_` is a LIKE wildcard,
    # so escape it (`\_`) to bind the namespace exactly.
    assert_scoped(
        f"SELECT name FROM system.tables WHERE database = '{CATALOG_NAME}' AND name LIKE '{escape_like_literal(namespace_1)}.%' ORDER BY name "
        "SETTINGS show_data_lake_catalogs_in_system_tables = true",
        expected_ns1,
    )

    # `startsWith` pushdown, pinned directly: the analyzer rewrites perfect-prefix
    # `name LIKE 'prefix%'` to `startsWith(name, 'prefix')`, which must also scope.
    assert_scoped(
        f"SELECT name FROM system.tables WHERE database = '{CATALOG_NAME}' AND startsWith(name, '{namespace_1}.') ORDER BY name "
        "SETTINGS show_data_lake_catalogs_in_system_tables = true",
        expected_ns1,
    )

    # The same query written as `LIKE`, with the rewrite forced on, to guard the
    # analyzer-rewrite path end-to-end even if the default flips in the future.
    assert_scoped(
        f"SELECT name FROM system.tables WHERE database = '{CATALOG_NAME}' AND name LIKE '{escape_like_literal(namespace_1)}.%' ORDER BY name "
        "SETTINGS show_data_lake_catalogs_in_system_tables = true, optimize_rewrite_like_perfect_affix = 1",
        expected_ns1,
    )

    # Equality pushdown for a fully-qualified table name.
    one_table = f"{namespace_1}.{namespace_1_tables[0]}"
    assert_scoped(
        f"SELECT name FROM system.tables WHERE database = '{CATALOG_NAME}' AND name = '{one_table}' ORDER BY name "
        "SETTINGS show_data_lake_catalogs_in_system_tables = true",
        one_table,
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


def test_optimize_manifest_with_catalog(started_cluster):
    # OPTIMIZE TABLE ... MANIFEST on a catalog-managed table must consolidate the per-insert manifests
    # and commit the new snapshot back through the catalog, without changing the data.
    node = started_cluster.instances["node1"]

    test_ref = f"test_optimize_manifest_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(root_namespace)
    # Unpartitioned table, so every per-insert data manifest can consolidate into a single one.
    create_table(catalog, root_namespace, table_name, DEFAULT_SCHEMA, PartitionSpec(), DEFAULT_SORT_ORDER)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    table_ref = f"{CATALOG_NAME}.`{root_namespace}.{table_name}`"
    write_settings = {"allow_insert_into_iceberg": 1, "write_full_path_in_iceberg_metadata": 1}

    # Several separate inserts -> several snapshots, each adding its own data manifest.
    num_inserts = 5
    for i in range(num_inserts):
        node.query(
            f"INSERT INTO {table_ref} VALUES (NULL, 'sym{i}', {100 + i}, {200 + i}, tuple('bot'));",
            settings=write_settings,
        )

    def current_snapshot_id():
        # Read the current snapshot from the catalog's metadata.json (avoids parsing the manifest-list
        # Avro, which pyiceberg rejects because ClickHouse omits field-ids there).
        table = catalog.load_table(f"{root_namespace}.{table_name}")
        assert table.current_snapshot() is not None, "expected a current snapshot after inserts"
        return table.metadata.current_snapshot_id

    snapshot_id_before = current_snapshot_id()
    rows_before = node.query(f"SELECT symbol, bid, ask FROM {table_ref} ORDER BY ALL")

    node.query(
        f"OPTIMIZE TABLE {table_ref} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
            "allow_insert_into_iceberg": 1,
            "write_full_path_in_iceberg_metadata": 1,
        },
    )

    # The compaction must commit a new (replace) snapshot back through the catalog.
    assert current_snapshot_id() != snapshot_id_before, (
        "OPTIMIZE TABLE ... MANIFEST did not commit a new snapshot through the catalog"
    )

    # The metadata-only rewrite must not change the data.
    rows_after = node.query(f"SELECT symbol, bid, ask FROM {table_ref} ORDER BY ALL")
    assert rows_after == rows_before


@pytest.mark.parametrize(
    "fields_to_remove",
    [
        ["snapshots"],
        ["metadata-log"],
        ["snapshot-log"],
        ["snapshots", "metadata-log", "snapshot-log"],
    ],
)
def test_insert_into_table_without_optional_metadata_arrays(started_cluster, fields_to_remove):
    # The Iceberg spec marks snapshots / metadata-log / snapshot-log as optional, so external
    # engines may create empty-table metadata that omits any of them. Inserting into such a table
    # must still succeed instead of aborting in the metadata write path.
    node = started_cluster.instances["node1"]

    test_ref = f"test_insert_no_optional_arrays_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(root_namespace)
    create_table(catalog, root_namespace, table_name, DEFAULT_SCHEMA, PartitionSpec(), DEFAULT_SORT_ORDER)

    iceberg_table = catalog.load_table(f"{root_namespace}.{table_name}")
    assert iceberg_table.metadata_location.startswith("s3://")
    metadata_bucket, metadata_key = iceberg_table.metadata_location[len("s3://"):].split("/", 1)
    metadata = json.loads(get_file_contents(started_cluster.minio_client, metadata_bucket, metadata_key))
    for field in fields_to_remove:
        metadata.pop(field, None)
    metadata_bytes = json.dumps(metadata).encode()
    started_cluster.minio_client.put_object(
        metadata_bucket,
        metadata_key,
        io.BytesIO(metadata_bytes),
        len(metadata_bytes),
        content_type="application/json",
    )

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)
    node.query(
        f"INSERT INTO {CATALOG_NAME}.`{root_namespace}.{table_name}` VALUES (NULL, 'AAPL', 193.24, 193.31, tuple('bot'));",
        settings={"allow_insert_into_iceberg": 1, "write_full_path_in_iceberg_metadata": 1},
    )
    assert node.query(f"SELECT * FROM {CATALOG_NAME}.`{root_namespace}.{table_name}`") == "\\N\tAAPL\t193.24\t193.31\t('bot')\n"


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
        ## getFilteredTables with engine_column populated (a former crash site). The table
        ## whose storage object could not be resolved is now KEPT in the listing with an empty
        ## engine, rather than being silently dropped.
        result = node.query(
            f"SELECT name, engine FROM system.tables WHERE database = '{CATALOG_NAME}' "
            f"SETTINGS show_data_lake_catalogs_in_system_tables = 1"
        )
        assert table_name in result
        ## engine is empty for the unresolved table (nothing after the name + tab).
        assert f"{table_name}\t" in result or f"{table_name}." in result

        ## fillData main loop path: the row is present (not dropped) even though every
        ## storage-dependent column is defaulted.
        result = node.query(
            f"SELECT count() FROM system.tables WHERE database = '{CATALOG_NAME}' "
            f"SETTINGS show_data_lake_catalogs_in_system_tables = 1"
        )
        assert int(result.strip()) >= 1

        ## A predicate on engine still filters correctly: the unresolved table has an empty
        ## engine, so it does not match a concrete engine pattern.
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


def test_system_tables_metadata_unresolvable_does_not_abort_scan(started_cluster):
    """
    Regression test for https://github.com/ClickHouse/ClickHouse/issues/110032.

    When a table's metadata is unresolvable, a system.tables scan of the whole
    DataLakeCatalog database must not abort (with database_datalake_require_metadata_access=1)
    nor silently drop the table (with =0). Either way the table stays listed by name, with
    default/empty values for the storage-dependent columns.
    """
    node = started_cluster.instances["node1"]

    root_namespace = f"clickhouse_{uuid.uuid4()}"
    namespace = f"{root_namespace}_test_unresolvable"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)

    table_name = "broken_table"
    create_table(catalog, namespace, table_name)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    ## Simulate a per-table metadata resolution failure (throws).
    node.query("SYSTEM ENABLE FAILPOINT datalake_try_get_table_throw")

    try:
        for require in (1, 0):
            settings = (
                f"SETTINGS show_data_lake_catalogs_in_system_tables = 1, "
                f"database_datalake_require_metadata_access = {require}"
            )

            ## Name-only fast path always worked; still lists the table.
            result = node.query(
                f"SELECT name FROM system.tables WHERE database = '{CATALOG_NAME}' {settings}"
            )
            assert table_name in result, f"name-only path, require={require}"

            ## The whole-database scan requesting a storage-dependent column must NOT abort
            ## and must NOT drop the table -- it is kept with an empty engine.
            result = node.query(
                f"SELECT name, engine FROM system.tables WHERE database = '{CATALOG_NAME}' {settings}"
            )
            assert table_name in result, f"full scan, require={require}"

            result = node.query(
                f"SELECT count() FROM system.tables WHERE database = '{CATALOG_NAME}' {settings}"
            )
            assert int(result.strip()) >= 1, f"count, require={require}"

            ## total_rows (a per-column stat that needs the opened storage) is defaulted, not fatal.
            result = node.query(
                f"SELECT count() FROM system.tables WHERE database = '{CATALOG_NAME}' "
                f"AND total_rows IS NULL {settings}"
            )
            assert int(result.strip()) >= 1, f"total_rows default, require={require}"

            ## parameterized_view_parameters needs the opened storage too. Selecting it alongside
            ## other columns must keep the column aligned (defaulted to an empty array), not abort.
            result = node.query(
                f"SELECT name, parameterized_view_parameters FROM system.tables "
                f"WHERE database = '{CATALOG_NAME}' {settings}"
            )
            assert table_name in result, f"parameterized_view_parameters scan, require={require}"

            result = node.query(
                f"SELECT count() FROM system.tables WHERE database = '{CATALOG_NAME}' "
                f"AND empty(parameterized_view_parameters) {settings}"
            )
            assert int(result.strip()) >= 1, f"parameterized_view_parameters default, require={require}"

            ## create_table_query / engine_full / as_select re-enter the catalog metadata query
            ## for a null-storage row. Selecting them must not re-throw and abort the scan; the
            ## columns are defaulted to empty strings for the unresolvable table.
            result = node.query(
                f"SELECT name, create_table_query, engine_full, as_select FROM system.tables "
                f"WHERE database = '{CATALOG_NAME}' {settings}"
            )
            assert table_name in result, f"create_table_query scan, require={require}"

            result = node.query(
                f"SELECT count() FROM system.tables WHERE database = '{CATALOG_NAME}' "
                f"AND create_table_query = '' AND engine_full = '' AND as_select = '' {settings}"
            )
            assert int(result.strip()) >= 1, f"create_table_query default, require={require}"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT datalake_try_get_table_throw")

    ## Direct access to the broken table still surfaces the error (query_and_get_error already
    ## asserts the query failed; here we check it is the injected metadata failure).
    node.query("SYSTEM ENABLE FAILPOINT datalake_try_get_table_throw")
    try:
        assert "Injected metadata resolution failure" in node.query_and_get_error(
            f"SELECT * FROM {CATALOG_NAME}.`{namespace}.{table_name}` "
            f"SETTINGS database_datalake_require_metadata_access = 1"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT datalake_try_get_table_throw")

    node.query(f"DROP DATABASE IF EXISTS {CATALOG_NAME}")


def test_merge_over_datalake_with_unresolvable_table_does_not_hang(started_cluster):
    """
    Regression test for the StorageMerge consumer of DatabaseDataLake::getTablesIterator.

    Only system.tables (getTablesIteratorWithHint) keeps a row with a null storage object
    for a table whose metadata is unresolvable. Every other consumer -- StorageMerge in
    particular -- dereferences the storage object of every iterated row unconditionally
    (ReadFromMerge::getSelectedTables skips null storage with `continue` without advancing
    the iterator, so it would loop forever; traverseTablesUntil callers such as
    supportsPrewhere / totalRows deref the table directly). getTablesIterator therefore must
    NOT yield null-storage rows: it propagates the error when
    database_datalake_require_metadata_access=1 and drops the unresolved table otherwise.

    A SELECT through a Merge table over the catalog with one broken table must fail cleanly
    or return only the resolvable tables' rows, never hang or crash during planning.
    """
    node = started_cluster.instances["node1"]

    root_namespace = f"clickhouse_{uuid.uuid4()}"
    namespace = f"{root_namespace}_test_merge_unresolvable"

    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)

    table_name = "broken_table"
    create_table(catalog, namespace, table_name)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    ## An explicitly-created Merge table with a declared structure skips schema inference, so a
    ## SELECT through it reaches ReadFromMerge::getSelectedTables directly -- the read path the
    ## bot flagged, where a null-storage iterator row would spin forever (`if (!storage)
    ## continue;` never advances the iterator). (The merge() table function cannot exercise this
    ## because it forces schema inference first, which resolves/errors before the read path.)
    node.query("DROP TABLE IF EXISTS default.merge_over_datalake")
    node.query(
        f"CREATE TABLE default.merge_over_datalake (symbol Nullable(String)) "
        f"ENGINE = Merge('{CATALOG_NAME}', '.*broken_table.*')"
    )

    node.query("SYSTEM ENABLE FAILPOINT datalake_try_get_table_throw")

    ## Pre-fix, getTablesIterator handed StorageMerge a null-storage iterator row for the
    ## broken table; ReadFromMerge::getSelectedTables then spun forever (`if (!storage)
    ## continue;` never advances the iterator), so the SELECT hung. With the fix that row is
    ## never yielded to StorageMerge -- the error is propagated (require_metadata_access
    ## defaults to 1 in the storage context) instead. Either way the query must COMPLETE
    ## within the timeout (a hang would trip the timeout and fail the test) and the server
    ## must survive. A generous-but-bounded timeout converts the pre-fix hang into a clean
    ## test failure rather than hanging the whole job.
    def run_merge_select(require):
        ## Completes (error or result) within the timeout == no infinite loop; return the
        ## outcome text for a sanity assertion. A hang raises and fails the test.
        try:
            return node.query(
                "SELECT count() FROM default.merge_over_datalake "
                f"SETTINGS database_datalake_require_metadata_access = {require}",
                timeout=60,
            ).strip()
        except QueryRuntimeException as e:
            return str(e)

    try:
        for require in (1, 0):
            outcome = run_merge_select(require)
            ## Either a clean numeric result (unresolved table dropped -> 0 rows) or the
            ## injected metadata error -- never a hang, never a crash/LOGICAL_ERROR.
            assert (
                outcome.isdigit()
                or "Injected metadata resolution failure" in outcome
                or "metadata" in outcome
            ), f"require={require}: {outcome}"
            assert "LOGICAL_ERROR" not in outcome, f"require={require}: {outcome}"

            ## Server is still alive (i.e. no crash from a null-storage deref).
            assert node.query("SELECT 1").strip() == "1", f"require={require}"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT datalake_try_get_table_throw")
        node.query("DROP TABLE IF EXISTS default.merge_over_datalake")

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


def test_alter_database_settings_not_supported(started_cluster):
    node = started_cluster.instances["node1"]

    db_name = f"iceberg_alter_settings_{uuid.uuid4().hex}"
    create_clickhouse_iceberg_database(started_cluster, node, db_name)

    fake_token = f"fake_secret_token_{uuid.uuid4().hex}"

    qid_alter = uuid.uuid4().hex
    error = node.query_and_get_error(
        f"ALTER DATABASE {db_name} MODIFY SETTING warehouse = 'other_warehouse'"
    )
    assert "BAD_ARGUMENTS" in error
    error = node.query_and_get_error(
        f"ALTER DATABASE {db_name} MODIFY SETTING onelake_bearer_token = '{fake_token}'",
        query_id=qid_alter,
    )
    assert "BAD_ARGUMENTS" in error

    error = node.query_and_get_error(
        f"ALTER DATABASE {db_name} MODIFY SETTING no_such_setting = 1"
    )
    assert "BAD_ARGUMENTS" in error or "UNKNOWN_SETTING" in error

    show_result = node.query(f"SHOW CREATE DATABASE {db_name}")
    assert "other_warehouse" not in show_result
    assert "onelake_bearer_token" not in show_result
    node.query(
        f"SELECT name FROM system.tables WHERE database = '{db_name}' SETTINGS show_data_lake_catalogs_in_system_tables = true"
    )

    node.query("SYSTEM FLUSH LOGS system.query_log")
    logged_query = node.query(
        f"SELECT arrayStringConcat(groupArray(query), '\\n') FROM system.query_log WHERE query_id = '{qid_alter}'"
    )
    assert fake_token not in logged_query
    assert "[HIDDEN]" in logged_query

    node.query(f"DROP DATABASE {db_name}")

    glue_db_name = f"glue_alter_settings_{uuid.uuid4().hex}"
    node.query(
        f"""
        ATTACH DATABASE {glue_db_name} ENGINE = DataLakeCatalog('http://fake-glue:1')
        SETTINGS catalog_type = 'glue', region = 'us-east-1', storage_endpoint = 'http://fake-glue:1/x'
        """
    )
    error = node.query_and_get_error(
        f"ALTER DATABASE {glue_db_name} MODIFY SETTING region = 'eu-west-1'"
    )
    assert "NOT_IMPLEMENTED" in error
    node.query(f"DROP DATABASE {glue_db_name}")


def test_alter_database_settings_rest_auth_header(started_cluster):
    node = started_cluster.instances["node1"]

    db_name = f"rest_alter_auth_header_{uuid.uuid4().hex}"
    old_header = f"Authorization: Bearer old_{uuid.uuid4().hex}"
    new_header = f"Authorization: Bearer new_{uuid.uuid4().hex}"

    node.query(
        f"""
        ATTACH DATABASE {db_name} ENGINE = DataLakeCatalog('http://fake-rest:1/api')
        SETTINGS catalog_type = 'rest', warehouse = 'wh', auth_header = '{old_header}'
        """
    )

    node.query(
        f"ALTER DATABASE {db_name} MODIFY SETTING auth_header = '{new_header}'"
    )

    error = node.query_and_get_error(
        f"ALTER DATABASE {db_name} MODIFY SETTING catalog_credential = 'id:secret'"
    )
    assert "BAD_ARGUMENTS" in error

    show_result = node.query(f"SHOW CREATE DATABASE {db_name}")
    assert new_header not in show_result
    assert "[HIDDEN]" in show_result

    node.restart_clickhouse()

    engine_full_with_secrets = node.query(
        f"SELECT engine_full FROM system.databases WHERE name = '{db_name}'",
        settings={"format_display_secrets_in_show_and_select": 1},
    )
    assert new_header in engine_full_with_secrets
    assert old_header not in engine_full_with_secrets

    node.query(f"DROP DATABASE {db_name}")


def test_alter_database_settings_onelake_persistence(started_cluster):
    node = started_cluster.instances["node1"]

    db_name = f"onelake_alter_persist_{uuid.uuid4().hex}"
    old_token = f"secret_token_{uuid.uuid4().hex}"
    new_token = f"secret_token_{uuid.uuid4().hex}"

    node.query(
        f"""
        ATTACH DATABASE {db_name} ENGINE = DataLakeCatalog('http://fake-onelake:1/api')
        SETTINGS catalog_type = 'onelake', warehouse = 'wh', onelake_tenant_id = 'tenant-0', onelake_tenant_id = 'tenant-1', onelake_bearer_token = '{old_token}'
        """
    )

    node.query(
        f"ALTER DATABASE {db_name} MODIFY SETTING onelake_tenant_id = 'tenant-2', onelake_bearer_token = '{new_token}'"
    )

    error = node.query_and_get_error(
        f"ALTER DATABASE {db_name} MODIFY SETTING onelake_client_id = 'client-1'"
    )
    assert "BAD_ARGUMENTS" in error

    error = node.query_and_get_error(
        f"ALTER DATABASE {db_name} MODIFY SETTING warehouse = 'other_warehouse'"
    )
    assert "BAD_ARGUMENTS" in error

    error = node.query_and_get_error(
        f"ALTER DATABASE {db_name} MODIFY SETTING onelake_bearer_token = ''"
    )
    assert "BAD_ARGUMENTS" in error

    show_result = node.query(f"SHOW CREATE DATABASE {db_name}")
    assert "tenant-2" in show_result
    assert new_token not in show_result
    assert old_token not in show_result
    assert "[HIDDEN]" in show_result

    engine_full_with_secrets = node.query(
        f"SELECT engine_full FROM system.databases WHERE name = '{db_name}'",
        settings={"format_display_secrets_in_show_and_select": 1},
    )
    assert "tenant-2" in engine_full_with_secrets
    assert new_token in engine_full_with_secrets
    assert old_token not in engine_full_with_secrets

    node.restart_clickhouse()

    show_result = node.query(f"SHOW CREATE DATABASE {db_name}")
    assert "tenant-2" in show_result
    assert "tenant-0" not in show_result
    assert "tenant-1" not in show_result
    assert new_token not in show_result
    assert "[HIDDEN]" in show_result

    engine_full = node.query(
        f"SELECT engine_full FROM system.databases WHERE name = '{db_name}'"
    )
    assert "tenant-2" in engine_full
    assert "tenant-0" not in engine_full
    assert "tenant-1" not in engine_full
    assert new_token not in engine_full

    engine_full_with_secrets = node.query(
        f"SELECT engine_full FROM system.databases WHERE name = '{db_name}'",
        settings={"format_display_secrets_in_show_and_select": 1},
    )
    assert new_token in engine_full_with_secrets
    assert old_token not in engine_full_with_secrets

    node.query(f"DROP DATABASE {db_name}")
