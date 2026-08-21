#!/usr/bin/env python3
"""Integration tests for the unified Unity Catalog (`catalog_type = 'unity_catalog'`).

`UnifiedUnityCatalog` serves both Delta and Iceberg tables from a single Unity
Catalog and detects the format per table. The open-source Unity Catalog server
used here can only express Delta tables: it never sends `securable_kind`, its
`DataSourceFormat` enum has no `ICEBERG` value, and it serves the Iceberg REST
API at `{base}/iceberg` rather than the `{base}/iceberg-rest` that Databricks
documents. The Iceberg tests therefore go through `mock_servers/uc_proxy.py`,
which patches exactly those two differences and nothing else.

The seeded UniForm table is registered under `/tmp`, so `configs/user_files_root.xml`
roots `user_files` at `/`. Reading the table where its own metadata says it lives
keeps every path inside it self-consistent.
"""

import json
import logging
import os
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

CATALOG = "unity"

UC_PORT = 8080
# 8081, the obvious choice, is taken: Unity Catalog binds both 8080 and 8081.
PROXY_PORT = 8090

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
DELTA_TABLE = "default.marksheet"

# `marksheet_uniform` is a UniForm copy of `marksheet` and holds byte-identical
# data, which lets the Iceberg arm be checked against the Delta arm.
SEEDED_ROW_COUNT = 15
SEEDED_FIRST_ROW = "1\tnWYHawtqUw\t930"
SEEDED_LAST_ROW = "15\tkxUUZEUoKv\t398"

EXPERIMENTAL_SETTING = "allow_experimental_database_unified_unity_catalog"


UC_HOME = "/var/lib/clickhouse/user_files/unitycatalog"
UC_LOG = UC_HOME + "/uc.log"
UC_START_TIMEOUT = 300


def start_unity_catalog(node):
    """Local variant of the one in `test_database_delta`. That one backgrounds the
    copy and the server together, so a failure to start is indistinguishable from
    a slow copy and leaves no log to read. Here the copy is synchronous and the
    server is checked for liveness, so a failure says what actually broke."""
    # The server's classpath (`server/target/classpath`) names 253 dependency
    # jars under `/root/.cache/coursier`, but outside CI the container runs as
    # uid 1000 / gid 0 and `/root` is mode 0700, so the JVM loads none of them.
    # `a+rx`, not `o+rx`: gid 0 matches the directory's group, whose bits win
    # over the other bits. Traversal is all that is needed; nothing writes there.
    node.exec_in_container(["bash", "-c", "chmod a+rx /root"], user="root")

    # `cp -r` fails here: eight sbt incremental-compile caches under */zinc are
    # mode 0600 and owned by root, while this runs as the container's own user.
    # tar skips them, and they have no role at runtime.
    node.exec_in_container(
        [
            "bash",
            "-c",
            # The server creates this directory itself only when `user_files_path`
            # points at it, and this test roots `user_files` at `/` instead.
            "mkdir -p /var/lib/clickhouse/user_files && "
            'tar -C / -cf - --exclude="*/zinc" unitycatalog'
            " | tar -C /var/lib/clickhouse/user_files -xf -",
        ]
    )
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"cd {UC_HOME} && setsid nohup bin/start-uc-server > {UC_LOG} 2>&1 < /dev/null &",
        ]
    )

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
        for description, command in (
            ("server process", "pgrep -af start-uc-server || echo '(not running)'"),
            ("log file", f"ls -la {UC_LOG} 2>&1"),
            ("log tail", f"tail -n 50 {UC_LOG} 2>&1"),
            ("java", "java -version 2>&1 | head -3"),
        ):
            output = node.exec_in_container(["bash", "-c", command], nothrow=True)
            print(f"Unity Catalog {description}:\n{output}")
        raise


UNIFORM_DIR = (
    "/var/lib/clickhouse/user_files/unitycatalog"
    "/etc/data/external/unity/default/tables/marksheet_uniform"
)


def link_uniform_table(node):
    """`marksheet_uniform` is the one sample table that needs setup before use.
    It ships registered at `file:///tmp/marksheet_uniform`, and the upstream docs
    (`docs/usage/tables/uniform.md`) tell you to copy the data there. A symlink
    does the same job for read-only data. Both readers need it: the Unity Catalog
    server reads the Iceberg metadata off disk at the registered path, and
    ClickHouse follows the same path once `user_files` is rooted at `/`."""
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"ln -sfn {UNIFORM_DIR} /tmp/marksheet_uniform && "
            "test -f /tmp/marksheet_uniform/metadata/"
            "00002-5b7aa739-d074-4764-b49d-ad6c63419576.metadata.json",
        ]
    )


PROXY_PATH = "/var/lib/clickhouse/user_files/uc_proxy.py"
PROXY_LOG = "/var/lib/clickhouse/user_files/uc_proxy.log"


def start_proxy(node):
    """`helpers.mock_servers.start_mock_servers` copies to a relative path, so the
    script lands in the container's working directory, which the container's uid
    cannot write to outside CI. Place it in `user_files` and start it directly."""
    node.copy_file_to_container(
        os.path.join(os.path.dirname(__file__), "mock_servers", "uc_proxy.py"),
        PROXY_PATH,
    )
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"setsid nohup python3 {PROXY_PATH} {PROXY_PORT} > {PROXY_LOG} 2>&1 < /dev/null &",
        ]
    )

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
    result = node.query(
        f"SHOW TABLES FROM {db_name} LIKE '{pattern}'",
        settings={"use_hive_partitioning": "0"},
    ).strip()
    return sorted(result.split("\n")) if result else []


def read_rows(node, db_name, table):
    result = node.query(
        f"SELECT * FROM {db_name}.`{table}` ORDER BY 1, 2, 3"
    ).strip()
    return result.split("\n") if result else []


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
    the Delta-only one: list every table and read it correctly."""
    node = started_cluster.instances["node1"]
    db_name = unique_name("unified_delta")
    create_database(node, db_name)

    assert show_tables(node, db_name, "default%") == SEEDED_TABLES

    assert "DeltaLake" in node.query(f"SHOW CREATE TABLE {db_name}.`{DELTA_TABLE}`")

    rows = read_rows(node, db_name, DELTA_TABLE)
    assert len(rows) == SEEDED_ROW_COUNT
    assert rows[0] == SEEDED_FIRST_ROW
    assert rows[-1] == SEEDED_LAST_ROW


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
    Iceberg REST endpoint, a completely different metadata path from Delta. The
    UniForm table is a copy of `marksheet`, so the two arms must agree row for
    row; that is a stronger check than either one on its own."""
    node = started_cluster.instances["node1"]
    iceberg_db = unique_name("unified_iceberg_read")
    delta_db = unique_name("unified_delta_read")
    create_database(node, iceberg_db, url=PROXY_URL)
    create_database(node, delta_db)

    assert UNIFORM_TABLE in show_tables(node, iceberg_db, "default%")

    described = node.query(f"DESCRIBE TABLE {iceberg_db}.`{UNIFORM_TABLE}`")
    assert "id\tNullable(Int32)" in described, described
    assert "name\tNullable(String)" in described, described
    assert "marks\tNullable(Int32)" in described, described

    through_iceberg = read_rows(node, iceberg_db, UNIFORM_TABLE)
    through_delta = read_rows(node, delta_db, DELTA_TABLE)

    assert len(through_iceberg) == SEEDED_ROW_COUNT
    assert through_iceberg == through_delta


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
