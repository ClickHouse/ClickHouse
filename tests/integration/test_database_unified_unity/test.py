#!/usr/bin/env python3
"""Integration tests for the unified Unity Catalog (`catalog_type = 'unity_catalog'`).

`UnifiedUnityCatalog` serves Delta and Iceberg tables from one catalog, detecting
the format per table.
"""

import json
import os
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

CATALOG = "unity"

UC_PORT = 8080
PROXY_PORT = 8090
UC_URL = f"http://localhost:{UC_PORT}/api/2.1/unity-catalog"
PROXY_URL = f"http://localhost:{PROXY_PORT}/api/2.1/unity-catalog"

# Seeded by the docker image, all Delta.
SEEDED_TABLES = [
    "default.marksheet",
    "default.marksheet_uniform",
    "default.numbers",
    "default.user_countries",
]
UNIFORM_TABLE = "default.marksheet_uniform"
DELTA_TABLE = "default.marksheet"

# `marksheet_uniform` is a UniForm copy of `marksheet`, byte-identical.
SEEDED_ROW_COUNT = 15
SEEDED_FIRST_ROW = "1\tnWYHawtqUw\t930"
SEEDED_LAST_ROW = "15\tkxUUZEUoKv\t398"

EXPERIMENTAL_SETTING = "allow_experimental_database_unified_unity_catalog"


UC_HOME = "/tmp/unitycatalog"
UC_LOG = UC_HOME + "/uc.log"
UC_START_TIMEOUT = 120


def start_unity_catalog(node):
    # Make root traversable so that non-root users can access classpath files.
    node.exec_in_container(["bash", "-c", "chmod a+rx /root"], user="root")

    # Copy from /unitycatalog to /tmp/unitycatalog.
    node.exec_in_container(
        [
            "bash",
            "-c",
            'tar -C / -cf - --exclude="*/zinc" unitycatalog | tar -C /tmp -xf -',
        ]
    )
    
    # Call start-uc-server.
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"cd {UC_HOME} && bin/start-uc-server > {UC_LOG} 2>&1 &",
        ]
    )

    # Wait for server to start.
    try:
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"for i in $(seq 1 {UC_START_TIMEOUT}); do "
                "(echo > /dev/tcp/localhost/8080) 2>/dev/null && exit 0; sleep 1; done; "
                f"echo 'Unity Catalog did not start within {UC_START_TIMEOUT}s' >&2; exit 1",
            ]
        )
    except Exception:
        # A bare port-wait timeout says nothing about why the server is absent.
        print(
            "Unity Catalog log:\n"
            + node.exec_in_container(
                ["bash", "-c", f"tail -n 50 {UC_LOG} 2>&1"], nothrow=True
            )
        )
        raise


def link_uniform_table(node):
    # `marksheet_uniform` is registered at /tmp/marksheet_uniform but is
    # stored in the catalog tree. Soft link  Soft link them.
    table_dir = UC_HOME + "/etc/data/external/unity/default/tables/marksheet_uniform"
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"ln -s {table_dir} /tmp/marksheet_uniform && "
            "test -d /tmp/marksheet_uniform/metadata",
        ]
    )


PROXY_PATH = "/tmp/uc_proxy.py"
PROXY_LOG = "/tmp/uc_proxy.log"


def start_proxy(node):
    # Copy uc_proxy.py to container.
    node.copy_file_to_container(
        os.path.join(os.path.dirname(__file__), "mock_servers", "uc_proxy.py"),
        PROXY_PATH,
    )

    # Start proxy.
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"python3 {PROXY_PATH} {PROXY_PORT} > {PROXY_LOG} 2>&1 &",
        ]
    )

    # Wait for proxy.
    try:
        node.exec_in_container(
            [
                "bash",
                "-c",
                "for i in $(seq 1 30); do "
                f"[ \"$(curl -s http://localhost:{PROXY_PORT}/)\" = OK ] && exit 0; sleep 1; done; "
                f"echo 'Proxy did not answer on port {PROXY_PORT}' >&2; exit 1",
            ]
        )
    except Exception:
        print(
            "Proxy log:\n"
            + node.exec_in_container(
                ["bash", "-c", f"tail -n 50 {PROXY_LOG} 2>&1"], nothrow=True
            )
        )
        raise


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=["configs/user_files_root.xml"],
            image="clickhouse/integration-test-with-unity-catalog",
            with_installed_binary=False,
            tag=os.environ.get("DOCKER_BASE_WITH_UNITY_CATALOG_TAG", "latest"),
        )

        cluster.start()

        node = cluster.instances["node1"]
        start_unity_catalog(node)
        link_uniform_table(node)
        start_proxy(node)

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
    result = node.query(f"SHOW TABLES FROM {db_name} LIKE '{pattern}'").strip()
    return sorted(result.split("\n")) if result else []


def assert_seeded_rows(node, db_name, table):
    """`marksheet` and its UniForm copy hold the same rows, whichever arm reads them."""
    rows = node.query(
        f"SELECT * FROM {db_name}.`{table}` ORDER BY 1, 2, 3"
    ).strip().split("\n")
    assert len(rows) == SEEDED_ROW_COUNT
    assert rows[0] == SEEDED_FIRST_ROW
    assert rows[-1] == SEEDED_LAST_ROW


def uc_api_post(node, route, payload):
    """Registers objects the seeded data does not provide, such as a CSV table."""
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
    """`CREATE DATABASE` must refuse without the opt-in setting."""
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
    """On an all-Delta catalog the unified engine must match the Delta-only one."""
    node = started_cluster.instances["node1"]
    db_name = unique_name("unified_delta")
    create_database(node, db_name)

    assert "DeltaLake" in node.query(f"SHOW CREATE TABLE {db_name}.`{DELTA_TABLE}`")

    assert_seeded_rows(node, db_name, DELTA_TABLE)


def test_unreadable_table_is_hidden(started_cluster):
    """An unreadable table is hidden from listings, and naming it says why."""
    node = started_cluster.instances["node1"]
    schema_name = unique_name("unified_unreadable")
    db_name = unique_name("unified_unreadable_db")

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
    node = started_cluster.instances["node1"]
    db_name = unique_name("unified_uniform")
    create_database(node, db_name)

    create_table = node.query(f"SHOW CREATE TABLE {db_name}.`{UNIFORM_TABLE}`")
    assert "DeltaLake" in create_table, create_table
    assert "Iceberg" not in create_table, create_table

    assert_seeded_rows(node, db_name, UNIFORM_TABLE)


def test_iceberg_table_routes_to_iceberg_arm(started_cluster):
    """Databricks reports managed Iceberg as `data_source_format = DELTA`, so
    `securable_kind` must win over the format."""
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
    """The Iceberg arm reads through an embedded `RestCatalog`, a different
    metadata path from Delta. Both arms read the same rows, so they must agree."""
    node = started_cluster.instances["node1"]
    iceberg_db = unique_name("unified_iceberg_read")
    create_database(node, iceberg_db, url=PROXY_URL)

    assert UNIFORM_TABLE in show_tables(node, iceberg_db, "default%")

    described = node.query(f"DESCRIBE TABLE {iceberg_db}.`{UNIFORM_TABLE}`")
    assert "id\tNullable(Int32)" in described, described
    assert "name\tNullable(String)" in described, described
    assert "marks\tNullable(Int32)" in described, described

    assert_seeded_rows(node, iceberg_db, UNIFORM_TABLE)


def test_mixed_formats_in_one_database(started_cluster):
    """One database serving both formats, which is the point of the engine."""
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

    delta_storages = used_storages(DELTA_TABLE)
    iceberg_storages = used_storages(UNIFORM_TABLE)

    assert "DeltaLake" in delta_storages, delta_storages
    assert "Iceberg" in iceberg_storages, iceberg_storages
