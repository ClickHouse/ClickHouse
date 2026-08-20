#!/usr/bin/env python3
"""Integration tests for the unified Unity Catalog (`catalog_type = 'unity_catalog'`).

`UnifiedUnityCatalog` serves both Delta and Iceberg tables from a single Unity
Catalog and detects the format per table. The open-source Unity Catalog server
used here can only express Delta tables: it never sends `securable_kind`, its
`DataSourceFormat` enum has no `ICEBERG` value, and it serves the Iceberg REST
API at `{base}/iceberg` rather than the `{base}/iceberg-rest` that Databricks
documents. The Iceberg tests therefore go through `mock_servers/uc_proxy.py`,
which patches exactly those two differences and nothing else.
"""

import json
import logging
import os
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers
from helpers.test_tools import TSV

# The Spark and Unity Catalog plumbing is shared with the Delta-only catalog test
# rather than duplicated: it carries retry and diagnostic logic for a chronic
# Spark JVM hang that is expensive to rediscover.
from test_database_delta.test import (
    execute_multiple_spark_queries,
    execute_spark_query,
    start_unity_catalog,
)

CATALOG = "unity"

UC_PORT = 8080
PROXY_PORT = 8081

UC_URL = f"http://localhost:{UC_PORT}/api/2.1/unity-catalog"
PROXY_URL = f"http://localhost:{PROXY_PORT}/api/2.1/unity-catalog"

# Seeded by the docker image, all Delta. `marksheet_uniform` is a UniForm table:
# a Delta table that also publishes Iceberg metadata.
SEEDED_TABLES = [
    "default.marksheet",
    "default.marksheet_uniform",
    "default.numbers",
    "default.user_countries",
]
UNIFORM_TABLE = "default.marksheet_uniform"

EXPERIMENTAL_SETTING = "allow_experimental_database_unified_unity_catalog"


UNIFORM_DIR = (
    "/var/lib/clickhouse/user_files/unitycatalog"
    "/etc/data/external/unity/default/tables/marksheet_uniform"
)


def link_uniform_table(node):
    """The seeded UniForm table is registered at `file:///tmp/marksheet_uniform`
    but its data ships elsewhere. The Unity Catalog server reads the Iceberg
    metadata from that registered path, so it needs the link. ClickHouse never
    sees a `/tmp` path: the proxy rewrites every location it is given."""
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"ln -sfn {UNIFORM_DIR} /tmp/marksheet_uniform && "
            "test -f /tmp/marksheet_uniform/metadata/"
            "00002-5b7aa739-d074-4764-b49d-ad6c63419576.metadata.json",
        ]
    )


def start_proxy(cluster):
    start_mock_servers(
        cluster,
        os.path.join(os.path.dirname(__file__), "mock_servers"),
        [("uc_proxy.py", "node1", PROXY_PORT)],
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=[],
            user_configs=[],
            image="clickhouse/integration-test-with-unity-catalog",
            with_installed_binary=False,
            tag=os.environ.get("DOCKER_BASE_WITH_UNITY_CATALOG_TAG", "latest"),
        )

        logging.info("Starting cluster...")
        cluster.start()

        node = cluster.instances["node1"]
        if (
            int(
                node.query(
                    "SELECT count() FROM system.table_engines WHERE name = 'DeltaLake'"
                ).strip()
            )
            == 0
        ):
            pytest.skip("DeltaLake engine is not available")

        start_unity_catalog(node)
        link_uniform_table(node)
        start_proxy(cluster)

        yield cluster

    finally:
        cluster.shutdown()


def unique_name(prefix):
    return f"{prefix}_{uuid.uuid4()}".replace("-", "_")


def create_database(node, db_name, url=UC_URL):
    node.query(f"DROP DATABASE IF EXISTS {db_name}")
    node.query(
        f"""
CREATE DATABASE {db_name} ENGINE = DataLakeCatalog('{url}')
SETTINGS warehouse = '{CATALOG}', catalog_type = 'unity_catalog',
         vended_credentials = false
        """,
        settings={EXPERIMENTAL_SETTING: "1"},
    )


def show_tables(node, db_name, pattern):
    result = node.query(
        f"SHOW TABLES FROM {db_name} LIKE '{pattern}'",
        settings={"use_hive_partitioning": "0"},
    ).strip()
    return sorted(result.split("\n")) if result else []


def uc_api_post(node, route, payload):
    """Calls the Unity Catalog REST API directly. Used to register tables that
    the Spark connector cannot create, such as a non-Delta external table."""
    script = f"""
import json, urllib.request
request = urllib.request.Request(
    {UC_URL + "/" + route!r},
    data={json.dumps(payload)!r}.encode(),
    method="POST",
    headers={{"Content-Type": "application/json"}},
)
print(urllib.request.urlopen(request).status)
"""
    return node.exec_in_container(["python3", "-c", script])


def test_experimental_gate(started_cluster):
    """The catalog is experimental, so `CREATE DATABASE` must refuse without the
    opt-in setting. Only a real CREATE is gated; ATTACH deliberately is not."""
    node = started_cluster.instances["node1"]
    db_name = unique_name("gated")

    error = node.query_and_get_error(
        f"""
CREATE DATABASE {db_name} ENGINE = DataLakeCatalog('{UC_URL}')
SETTINGS warehouse = '{CATALOG}', catalog_type = 'unity_catalog', vended_credentials = false
        """
    )
    assert EXPERIMENTAL_SETTING in error, error


def test_list_and_read_delta_tables(started_cluster):
    """The seeded catalog is all Delta, so the unified catalog must behave like
    the Delta-only one: list every table and read it identically to Spark."""
    node = started_cluster.instances["node1"]
    db_name = unique_name("unified_delta")
    create_database(node, db_name)

    assert show_tables(node, db_name, "default%") == SEEDED_TABLES

    # `marksheet` and `user_countries` are the two seeded tables the Delta-only
    # test also reads; the rest are covered by the listing assertion above.
    for table in ("default.marksheet", "default.user_countries"):
        assert "DeltaLake" in node.query(f"SHOW CREATE TABLE {db_name}.`{table}`")

        from_clickhouse = TSV(
            node.query(f"SELECT * FROM {db_name}.`{table}` ORDER BY 1, 2, 3")
        )
        from_spark = TSV(
            execute_spark_query(node, f"SELECT * FROM {CATALOG}.{table} ORDER BY 1, 2, 3")
        )
        assert from_clickhouse == from_spark


def test_multiple_schemas(started_cluster):
    """Schema and table listing are paginated, and an intermediate empty page
    used to end the listing early. Several schemas exercise both loops."""
    node = started_cluster.instances["node1"]
    test_uuid = str(uuid.uuid4()).replace("-", "_")
    db_name = f"unified_schemas_{test_uuid}"

    schemas = [f"unified_schema_{test_uuid}_{i}" for i in range(3)]

    # One Spark invocation for every statement: a JVM start costs far more than
    # the statements themselves. Every statement is idempotent, so a fresh-JVM
    # retry after a partial commit converges to the same state.
    queries = []
    for i, schema in enumerate(schemas):
        queries.extend(
            [
                f"CREATE SCHEMA IF NOT EXISTS {schema}",
                f"CREATE TABLE IF NOT EXISTS {schema}.t (col1 int, col2 double) "
                f"USING Delta LOCATION '/var/lib/clickhouse/user_files/tmp/{schema}/t'",
                f"INSERT OVERWRITE {schema}.t VALUES ({i}, {i}.0)",
            ]
        )
    execute_multiple_spark_queries(node, queries, retry_on_timeout=True)

    create_database(node, db_name)

    tables = show_tables(node, db_name, f"unified_schema_{test_uuid}%")
    assert tables == [f"{schema}.t" for schema in schemas]

    for i, table in enumerate(tables):
        assert node.query(f"SELECT col1 FROM {db_name}.`{table}`").strip() == str(i)


def test_unreadable_table_is_hidden(started_cluster):
    """A table whose `data_source_format` is neither Delta nor Iceberg is not
    readable. It must be absent from listings, and naming it directly must
    report why rather than fail obscurely."""
    node = started_cluster.instances["node1"]
    test_uuid = str(uuid.uuid4()).replace("-", "_")
    schema_name = f"unified_unreadable_{test_uuid}"
    db_name = f"unified_unreadable_db_{test_uuid}"

    uc_api_post(node, "schemas", {"name": schema_name, "catalog_name": CATALOG})
    uc_api_post(
        node,
        "tables",
        {
            "name": "csv_table",
            "catalog_name": CATALOG,
            "schema_name": schema_name,
            "table_type": "EXTERNAL",
            "data_source_format": "CSV",
            # `setLocation` requires a `://` scheme and throws `Unexpected location
            # format` without one, which would pre-empt the format check under test.
            "storage_location": f"file:///var/lib/clickhouse/user_files/tmp/{schema_name}/csv_table",
            "columns": [
                {
                    "name": "id",
                    "type_text": "int",
                    "type_json": json.dumps(
                        {
                            "name": "id",
                            "type": "integer",
                            "nullable": True,
                            "metadata": {},
                        }
                    ),
                    "type_name": "INT",
                    "type_precision": 0,
                    "type_scale": 0,
                    "position": 0,
                    "nullable": True,
                }
            ],
        },
    )

    create_database(node, db_name)

    assert show_tables(node, db_name, f"{schema_name}%") == []

    error = node.query_and_get_error(
        f"SELECT * FROM {db_name}.`{schema_name}.csv_table`"
    )
    assert "as Delta because it has data_source_format 'CSV'" in error, error


def test_uniform_table_reads_as_delta(started_cluster):
    """The control case for the Iceberg tests below. Unpatched, the UniForm
    table reports `data_source_format = DELTA` and must route to the Delta arm."""
    node = started_cluster.instances["node1"]
    db_name = unique_name("unified_uniform")
    create_database(node, db_name)

    create_table = node.query(f"SHOW CREATE TABLE {db_name}.`{UNIFORM_TABLE}`")
    assert "DeltaLake" in create_table, create_table
    assert "Iceberg" not in create_table, create_table


def test_iceberg_table_routes_to_iceberg_arm(started_cluster):
    """Databricks reports a managed Iceberg table with `data_source_format = DELTA`
    and an Iceberg `securable_kind`, so the kind must win over the format. The
    proxy injects the kind; the same table read directly stays Delta."""
    node = started_cluster.instances["node1"]
    proxied_db = unique_name("unified_iceberg")
    direct_db = unique_name("unified_direct")

    create_database(node, proxied_db, url=PROXY_URL)
    create_database(node, direct_db)

    proxied = node.query(f"SHOW CREATE TABLE {proxied_db}.`{UNIFORM_TABLE}`")
    direct = node.query(f"SHOW CREATE TABLE {direct_db}.`{UNIFORM_TABLE}`")

    assert "Iceberg" in proxied, proxied
    assert "DeltaLake" in direct, direct


def test_iceberg_table_is_listed_and_readable(started_cluster):
    """Reading through the Iceberg arm goes to an embedded `RestCatalog` at the
    Iceberg REST endpoint, a completely different metadata path from Delta."""
    node = started_cluster.instances["node1"]
    db_name = unique_name("unified_iceberg_read")
    create_database(node, db_name, url=PROXY_URL)

    assert UNIFORM_TABLE in show_tables(node, db_name, "default%")

    assert node.query(f"DESCRIBE TABLE {db_name}.`{UNIFORM_TABLE}`").strip() != ""

    from_clickhouse = TSV(
        node.query(f"SELECT * FROM {db_name}.`{UNIFORM_TABLE}` ORDER BY 1, 2, 3")
    )
    from_spark = TSV(
        execute_spark_query(
            node, f"SELECT * FROM {CATALOG}.{UNIFORM_TABLE} ORDER BY 1, 2, 3"
        )
    )
    assert from_clickhouse == from_spark


def test_mixed_formats_in_one_database(started_cluster):
    """The reason the unified catalog exists: one database serving both formats.
    `used_storages` from the query log names the engine each read actually used."""
    node = started_cluster.instances["node1"]
    db_name = unique_name("unified_mixed")
    create_database(node, db_name, url=PROXY_URL)

    assert show_tables(node, db_name, "default%") == SEEDED_TABLES

    def used_storages(table):
        query_id = str(uuid.uuid4()).replace("-", "")
        node.query(f"SELECT * FROM {db_name}.`{table}` LIMIT 1", query_id=query_id)
        node.query("SYSTEM FLUSH LOGS")
        return node.query(
            "SELECT used_storages FROM system.query_log"
            f" WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        ).strip()

    delta_storages = used_storages("default.marksheet")
    iceberg_storages = used_storages(UNIFORM_TABLE)

    assert "DeltaLake" in delta_storages, delta_storages
    assert "Iceberg" in iceberg_storages, iceberg_storages
