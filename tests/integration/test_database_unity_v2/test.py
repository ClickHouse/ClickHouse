#!/usr/bin/env python3
"""Integration tests for the new Unity catalog implementation
(`catalog_type = 'unity'` with `use_unity_catalog_v2 = 1`).

`UnityV2Catalog` serves Delta and Iceberg tables from one catalog, detecting
the format per table.
"""

import json
import os
import shlex
import urllib.parse
import uuid
from contextlib import contextmanager

import pytest
from minio import Minio
from minio.error import S3Error

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key
from helpers.s3_tools import minio_admin, minio_user

CATALOG = "unity"

UC_PORT = 8080
PROXY_PORT = 8090
UC_URL = f"http://localhost:{UC_PORT}/api/2.1/unity-catalog"
PROXY_URL = f"http://localhost:{PROXY_PORT}/api/2.1/unity-catalog"

# Must match the constants in mock_servers/uc_proxy.py.
CLIENT_ID = "test-client"
CLIENT_SECRET = "test-secret"
PAT_TOKEN = "dapi-test-pat"

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

GATE_SETTING = "allow_database_unity_catalog"
V2_SETTING = "use_unity_catalog_v2"

DELTA_WRITE_SETTINGS = {
    "allow_experimental_delta_lake_writes": 1,
    "allow_delta_lake_create_table": 1,
}


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
    # stored in the catalog tree. Soft link them.
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
            f"python3 {PROXY_PATH} {PROXY_PORT} {shlex.quote(minio_access_key)} {shlex.quote(minio_secret_key)}"
            f" > {PROXY_LOG} 2>&1 &",
        ]
    )

    # Wait for proxy.
    try:
        node.exec_in_container(
            [
                "bash",
                "-c",
                "for i in $(seq 1 30); do "
                f'[ "$(curl -s http://localhost:{PROXY_PORT}/)" = OK ] && exit 0; sleep 1; done; '
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
            main_configs=["configs/user_files_root.xml", "configs/display_secrets.xml"],
            image="clickhouse/integration-test-with-unity-catalog",
            with_installed_binary=False,
            stay_alive=True,
            with_minio=True,
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


def skip_if_no_delta_kernel(node):
    # The Delta kernel (Rust) is not built under Memory Sanitizer, so the DeltaLake engine is absent.
    has_delta_lake = (
        int(
            node.query(
                "SELECT count() FROM system.table_engines WHERE name = 'DeltaLake'"
            ).strip()
        )
        > 0
    )
    if not has_delta_lake:
        pytest.skip("Build does not support DeltaLake (Delta kernel is unavailable)")


def unique_name(prefix):
    return f"{prefix}_{uuid.uuid4()}".replace("-", "_")


def create_database(node, db_name, url=UC_URL, catalog_credential=None):
    credential_clause = (
        f",\n         catalog_credential = '{catalog_credential}'"
        if catalog_credential is not None
        else ""
    )
    node.query(f"DROP DATABASE IF EXISTS {db_name}")
    node.query(
        f"""
CREATE DATABASE {db_name} ENGINE = DataLakeCatalog('{url}')
SETTINGS warehouse = '{CATALOG}', catalog_type = 'unity', {V2_SETTING} = 1,
         vended_credentials = false{credential_clause}
        """,
        settings={GATE_SETTING: "1"},
    )


def proxy_control(node, route):
    """The proxy's unauthenticated side channel for the tests."""
    return node.exec_in_container(
        ["curl", "-s", f"http://localhost:{PROXY_PORT}/control/{route}"]
    )


def vend_credentials(node, access_key=minio_access_key, secret_key=minio_secret_key):
    """Without arguments, restores the MinIO root credentials."""
    query = urllib.parse.urlencode({"access_key": access_key, "secret_key": secret_key})
    proxy_control(node, f"vend?{query}")


def show_tables(node, db_name, pattern):
    result = node.query(f"SHOW TABLES FROM {db_name} LIKE '{pattern}'").strip()
    return sorted(result.split("\n")) if result else []


def assert_seeded_rows(node, db_name, table):
    """`marksheet` and its UniForm copy hold the same rows, whichever arm reads them."""
    rows = (
        node.query(f"SELECT * FROM {db_name}.`{table}` ORDER BY 1, 2, 3")
        .strip()
        .split("\n")
    )
    assert len(rows) == SEEDED_ROW_COUNT
    assert rows[0] == SEEDED_FIRST_ROW
    assert rows[-1] == SEEDED_LAST_ROW


def uc_api_post(node, route, payload):
    """Registers objects the seeded data does not provide, such as a CSV table."""
    return uc_api_request(node, "POST", route, json.dumps(payload).encode())


def uc_api_delete(node, route):
    return uc_api_request(node, "DELETE", route)


def uc_api_request(node, method, route, data=None):
    script = f"""
import json, urllib.request
request = urllib.request.Request(
    {UC_URL + "/" + route!r},
    data={data!r},
    method={method!r},
    headers={{"Content-Type": "application/json"}},
)
print(urllib.request.urlopen(request).status)
"""
    return node.exec_in_container(["python3", "-c", script])


def test_experimental_gate(started_cluster):
    """`CREATE DATABASE` must refuse without the Unity opt-in setting."""
    node = started_cluster.instances["node1"]
    db_name = unique_name("gated")

    error = node.query_and_get_error(f"""
CREATE DATABASE {db_name} ENGINE = DataLakeCatalog('{UC_URL}')
SETTINGS warehouse = '{CATALOG}', catalog_type = 'unity', {V2_SETTING} = 1, vended_credentials = false
        """)
    assert GATE_SETTING in error


def create_legacy_database(node, db_name):
    """`CREATE` through the proxy, pinned to the legacy implementation."""
    node.query(f"DROP DATABASE IF EXISTS {db_name}")
    node.query(
        f"""
CREATE DATABASE {db_name} ENGINE = DataLakeCatalog('{PROXY_URL}')
SETTINGS warehouse = '{CATALOG}', catalog_type = 'unity', {V2_SETTING} = 0,
         vended_credentials = false, catalog_credential = '{PAT_TOKEN}'
        """,
        settings={GATE_SETTING: "1"},
    )


def assert_legacy_hides_iceberg(node, db_name):
    """The legacy implementation does not read tables with an Iceberg `securable_kind`,
    which the proxy stamps on the UniForm table, so it hides them."""
    assert UNIFORM_TABLE not in show_tables(node, db_name, "default%")
    assert DELTA_TABLE in show_tables(node, db_name, "default%")


def test_alter_switches_implementation(started_cluster):
    """An existing legacy database is migrated with `ALTER DATABASE ... MODIFY SETTING`."""
    node = started_cluster.instances["node1"]
    db_name = unique_name("alter_v2")
    create_legacy_database(node, db_name)
    assert_legacy_hides_iceberg(node, db_name)

    node.query(f"ALTER DATABASE {db_name} MODIFY SETTING {V2_SETTING} = 1")

    assert f"{V2_SETTING} = 1" in node.query(f"SHOW CREATE DATABASE {db_name}")
    assert "Iceberg" in node.query(f"SHOW CREATE TABLE {db_name}.`{UNIFORM_TABLE}`")
    assert_seeded_rows(node, db_name, UNIFORM_TABLE)

    # Re-applying the current value on an initialized catalog is a no-op.
    node.query(f"ALTER DATABASE {db_name} MODIFY SETTING {V2_SETTING} = 1")
    assert "Iceberg" in node.query(f"SHOW CREATE TABLE {db_name}.`{UNIFORM_TABLE}`")

    # The switch survives a restart, and it can be reverted.
    node.restart_clickhouse()
    assert "Iceberg" in node.query(f"SHOW CREATE TABLE {db_name}.`{UNIFORM_TABLE}`")

    node.query(f"ALTER DATABASE {db_name} MODIFY SETTING {V2_SETTING} = 0")
    assert V2_SETTING + " = 0" in node.query(f"SHOW CREATE DATABASE {db_name}")
    assert_legacy_hides_iceberg(node, db_name)

    # Only the implementation switch may be altered.
    error = node.query_and_get_error(
        f"ALTER DATABASE {db_name} MODIFY SETTING warehouse = 'other'"
    )
    assert "cannot be altered" in error


def test_list_and_read_delta_tables(started_cluster):
    """On an all-Delta catalog the new implementation must match the legacy one."""
    node = started_cluster.instances["node1"]
    db_name = unique_name("v2_delta")
    create_database(node, db_name)

    assert "DeltaLake" in node.query(f"SHOW CREATE TABLE {db_name}.`{DELTA_TABLE}`")

    assert_seeded_rows(node, db_name, DELTA_TABLE)


def test_unreadable_table_is_hidden(started_cluster):
    """An unreadable table is hidden from listings, and naming it says why."""
    node = started_cluster.instances["node1"]
    schema_name = unique_name("v2_unreadable")
    db_name = unique_name("v2_unreadable_db")

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
    assert "as Delta because it has data_source_format 'CSV'" in error


def test_uniform_table_reads_as_delta(started_cluster):
    node = started_cluster.instances["node1"]
    skip_if_no_delta_kernel(node)

    db_name = unique_name("v2_uniform")
    create_database(node, db_name)

    create_table = node.query(f"SHOW CREATE TABLE {db_name}.`{UNIFORM_TABLE}`")
    assert "DeltaLake" in create_table
    assert "Iceberg" not in create_table

    assert_seeded_rows(node, db_name, UNIFORM_TABLE)


def test_iceberg_table_routes_to_iceberg_arm(started_cluster):
    """Databricks reports managed Iceberg as `data_source_format = DELTA`, so
    `securable_kind` must win over the format."""
    node = started_cluster.instances["node1"]
    proxied_db = unique_name("v2_iceberg")
    direct_db = unique_name("v2_direct")

    create_database(node, proxied_db, url=PROXY_URL, catalog_credential=PAT_TOKEN)
    create_database(node, direct_db)

    proxied = node.query(f"SHOW CREATE TABLE {proxied_db}.`{UNIFORM_TABLE}`")
    direct = node.query(f"SHOW CREATE TABLE {direct_db}.`{UNIFORM_TABLE}`")

    assert "Iceberg" in proxied
    assert "DeltaLake" in direct


def test_iceberg_table_is_listed_and_readable(started_cluster):
    """The Iceberg arm reads through an embedded `RestCatalog`, a different
    metadata path from Delta. Both arms read the same rows, so they must agree."""
    node = started_cluster.instances["node1"]
    iceberg_db = unique_name("v2_iceberg_read")
    create_database(node, iceberg_db, url=PROXY_URL, catalog_credential=PAT_TOKEN)

    assert UNIFORM_TABLE in show_tables(node, iceberg_db, "default%")

    described = node.query(f"DESCRIBE TABLE {iceberg_db}.`{UNIFORM_TABLE}`")
    assert "id\tNullable(Int32)" in described
    assert "name\tNullable(String)" in described
    assert "marks\tNullable(Int32)" in described

    assert_seeded_rows(node, iceberg_db, UNIFORM_TABLE)


def test_mixed_formats_in_one_database(started_cluster):
    """One database serving both formats, which is the point of the engine."""
    node = started_cluster.instances["node1"]
    db_name = unique_name("v2_mixed")
    create_database(node, db_name, url=PROXY_URL, catalog_credential=PAT_TOKEN)

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

    assert "DeltaLake" in delta_storages
    assert "Iceberg" in iceberg_storages


def test_create_and_insert_delta_table(started_cluster):
    """A table created through the database is registered in Unity, and rows
    written through the database read back."""
    node = started_cluster.instances["node1"]
    skip_if_no_delta_kernel(node)
    schema_name = unique_name("v2_write")
    table_name = "created"
    db_name = unique_name("v2_write_db")
    location = f"/var/lib/clickhouse/user_files/tmp/{schema_name}/{table_name}"

    # Unity rejects a table create into a missing schema.
    uc_api_post(node, "schemas", {"name": schema_name, "catalog_name": CATALOG})
    create_database(node, db_name)

    node.query(
        f"CREATE TABLE {db_name}.`{schema_name}.{table_name}` (id Int32, name String)"
        f" ENGINE = DeltaLakeLocal('{location}')",
        settings=DELTA_WRITE_SETTINGS,
    )
    node.query(
        f"INSERT INTO {db_name}.`{schema_name}.{table_name}` VALUES (1, 'a'), (2, 'b')",
        settings=DELTA_WRITE_SETTINGS,
    )

    assert f"{schema_name}.{table_name}" in show_tables(node, db_name, f"{schema_name}%")
    assert (
        node.query(f"SELECT * FROM {db_name}.`{schema_name}.{table_name}` ORDER BY id")
        == "1\ta\n2\tb\n"
    )

    # Commit 0 from CREATE, commit 1 from INSERT.
    commits = node.exec_in_container(
        ["bash", "-c", f"ls {location}/_delta_log/*.json"]
    ).split()
    assert len(commits) == 2, commits


@contextmanager
def vended_s3_delta_table(cluster, use_v2):
    """An empty external Delta table on MinIO, read through a database that uses vended credentials."""
    node = cluster.instances["node1"]
    schema_name = unique_name("vended")
    table_name = "on_s3"
    db_name = unique_name("vended_db")
    table_key = f"{schema_name}/{table_name}"
    minio_endpoint = f"http://{cluster.minio_host}:{cluster.minio_port}/"

    node.query(
        f"CREATE TABLE {schema_name} (id Int32, name String) ENGINE = DeltaLake("
        f"'{minio_endpoint}{cluster.minio_bucket}/{table_key}/', '{minio_access_key}', '{minio_secret_key}')",
        settings=DELTA_WRITE_SETTINGS,
    )
    node.query(f"DROP TABLE {schema_name}")

    uc_api_post(node, "schemas", {"name": schema_name, "catalog_name": CATALOG})
    uc_api_post(
        node,
        "tables",
        {
            "name": table_name,
            "catalog_name": CATALOG,
            "schema_name": schema_name,
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "storage_location": f"s3://{cluster.minio_bucket}/{table_key}",
            "columns": [
                {
                    "name": name,
                    "type_text": type_text,
                    "type_json": json.dumps(
                        {"name": name, "type": type_json, "nullable": True, "metadata": {}}
                    ),
                    "type_name": type_name,
                    "position": position,
                    "nullable": True,
                }
                for position, (name, type_text, type_json, type_name) in enumerate(
                    [("id", "int", "integer", "INT"), ("name", "string", "string", "STRING")]
                )
            ],
        },
    )

    node.query(f"DROP DATABASE IF EXISTS {db_name}")
    node.query(
        f"""
CREATE DATABASE {db_name} ENGINE = DataLakeCatalog('{PROXY_URL}')
SETTINGS warehouse = '{CATALOG}', catalog_type = 'unity', {V2_SETTING} = {use_v2},
         vended_credentials = true, catalog_credential = '{PAT_TOKEN}',
         storage_endpoint = '{minio_endpoint}'
        """,
        settings={GATE_SETTING: "1"},
    )

    try:
        yield f"{db_name}.`{schema_name}.{table_name}`"
    finally:
        node.query(f"DROP DATABASE IF EXISTS {db_name}")
        uc_api_delete(node, f"tables/{CATALOG}.{schema_name}.{table_name}")
        uc_api_delete(node, f"schemas/{CATALOG}.{schema_name}")


@pytest.mark.parametrize("use_v2", [0, 1])
def test_insert_requests_write_credentials(started_cluster, use_v2):
    """Unity vends read-only credentials for `READ`, so `INSERT` into an
    external Delta table on S3 must request `READ_WRITE` ones."""
    node = started_cluster.instances["node1"]
    skip_if_no_delta_kernel(node)
    with vended_s3_delta_table(started_cluster, use_v2) as table:
        proxy_control(node, "reset_credential_operations")
        node.query(f"SELECT count() FROM {table}")
        assert set(json.loads(proxy_control(node, "credential_operations"))) == {"READ"}

        proxy_control(node, "reset_credential_operations")
        node.query(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')", settings=DELTA_WRITE_SETTINGS)
        assert "READ_WRITE" in json.loads(proxy_control(node, "credential_operations"))

        assert node.query(f"SELECT * FROM {table} ORDER BY id") == "1\ta\n2\tb\n"


@pytest.mark.parametrize("use_v2", [0, 1])
def test_expired_vended_credentials_are_refreshed_mid_scan(started_cluster, use_v2):
    node = started_cluster.instances["node1"]
    skip_if_no_delta_kernel(node)
    with vended_s3_delta_table(started_cluster, use_v2) as table:
        node.query(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')", settings=DELTA_WRITE_SETTINGS)
        query = f"SELECT * FROM {table} ORDER BY id"
        proxy_control(node, "reset_credential_operations")
        node.query(query)
        requests_without_refresh = len(json.loads(proxy_control(node, "credential_operations")))
        proxy_control(node, "reset_credential_operations")

        # The first S3 read of the data fails as if the vended token had expired.
        query_id = uuid.uuid4().hex
        node.query("SYSTEM ENABLE FAILPOINT s3_send_request_throw_expired_token")
        try:
            assert node.query(query, query_id=query_id) == "1\ta\n2\tb\n"
        finally:
            node.query("SYSTEM DISABLE FAILPOINT s3_send_request_throw_expired_token")

        node.query("SYSTEM FLUSH LOGS query_log")
        read_errors = node.query(
            "SELECT ProfileEvents['ReadBufferFromS3RequestsErrors'] FROM system.query_log "
            f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
        assert int(read_errors) >= 1
        assert len(json.loads(proxy_control(node, "credential_operations"))) > requests_without_refresh


@pytest.mark.parametrize("use_v2", [0, 1])
def test_database_recovers_after_catalog_vends_bad_credentials(started_cluster, use_v2):
    node = started_cluster.instances["node1"]
    skip_if_no_delta_kernel(node)
    with vended_s3_delta_table(started_cluster, use_v2) as table:
        node.query(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')", settings=DELTA_WRITE_SETTINGS)

        vend_credentials(node, "invalid-key", "invalid-secret")
        try:
            error = node.query_and_get_error(f"SELECT count() FROM {table}")
        finally:
            vend_credentials(node)
        assert "invalid-key" in error or "Access Key" in error, error
        assert "invalid-secret" not in error

        assert node.query(f"SELECT * FROM {table} ORDER BY id") == "1\ta\n2\tb\n"


@pytest.mark.parametrize("use_v2", [0, 1])
def test_vended_credentials_rotated_between_queries(started_cluster, use_v2):
    node = started_cluster.instances["node1"]
    skip_if_no_delta_kernel(node)
    with (
        minio_user(started_cluster, ["s3:*"]) as (old_key, old_secret),
        minio_user(started_cluster, ["s3:*"]) as (new_key, new_secret),
        vended_s3_delta_table(started_cluster, use_v2) as table,
    ):
        try:
            vend_credentials(node, old_key, old_secret)
            node.query(f"INSERT INTO {table} VALUES (1, 'a')", settings=DELTA_WRITE_SETTINGS)
            assert node.query(f"SELECT * FROM {table}") == "1\ta\n"

            vend_credentials(node, new_key, new_secret)
            minio_admin(started_cluster).user_disable(old_key)
            old_client = Minio(
                f"{started_cluster.minio_ip}:{started_cluster.minio_port}",
                access_key=old_key,
                secret_key=old_secret,
                secure=False,
            )
            with pytest.raises(S3Error):
                old_client.bucket_exists(started_cluster.minio_bucket)

            # The same database keeps working: every query fetches fresh credentials.
            proxy_control(node, "reset_credential_operations")
            node.query(f"INSERT INTO {table} VALUES (2, 'b')", settings=DELTA_WRITE_SETTINGS)
            assert node.query(f"SELECT * FROM {table} ORDER BY id") == "1\ta\n2\tb\n"
            assert "READ_WRITE" in json.loads(proxy_control(node, "credential_operations"))
        finally:
            vend_credentials(node)


@pytest.mark.parametrize("use_v2", [0, 1])
def test_vended_credentials_rotated_in_the_middle_of_a_query(started_cluster, use_v2):
    node = started_cluster.instances["node1"]
    skip_if_no_delta_kernel(node)
    failpoint = "object_storage_source_pause_before_virtual_columns"
    # One source and large files: the pause lands in the first file, so the second file's requests
    # start after the old key is disabled. The query keeps the old key until a refresh, and MinIO
    # checks credentials when a request starts, so the first of them fails.
    write_settings = {**DELTA_WRITE_SETTINGS, "output_format_parquet_row_group_size": 10000}
    read_settings = {
        "max_threads": 1,
        "max_block_size": 8192,
        "max_read_buffer_size": 65536,
        "remote_filesystem_read_prefetch": 0,
    }
    with (
        minio_user(started_cluster, ["s3:*"]) as (old_key, old_secret),
        minio_user(started_cluster, ["s3:*"]) as (new_key, new_secret),
        vended_s3_delta_table(started_cluster, use_v2) as table,
    ):
        query_id = uuid.uuid4().hex
        try:
            vend_credentials(node, old_key, old_secret)
            for start in (0, 200000):
                node.query(
                    f"INSERT INTO {table} SELECT number, toString(number) FROM numbers({start}, 200000)",
                    settings=write_settings,
                )

            node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
            request = node.get_query_request(
                f"SELECT count(), sum(id) FROM {table}", query_id=query_id, settings=read_settings
            )
            node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)

            vend_credentials(node, new_key, new_secret)
            minio_admin(started_cluster).user_disable(old_key)
            proxy_control(node, "reset_credential_operations")
            # The failpoint pauses every chunk, so disable it before waiting for the answer.
            node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

            assert request.get_answer() == f"400000\t{sum(range(400000))}\n"
        finally:
            node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
            vend_credentials(node)

        node.query("SYSTEM FLUSH LOGS query_log")
        read_errors = node.query(
            "SELECT ProfileEvents['ReadBufferFromS3RequestsErrors'] FROM system.query_log "
            f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
        assert int(read_errors) >= 1
        assert "READ" in json.loads(proxy_control(node, "credential_operations"))


@pytest.mark.parametrize("use_v2", [0, 1])
def test_insert_with_read_only_vended_credentials_commits_nothing(started_cluster, use_v2):
    node = started_cluster.instances["node1"]
    skip_if_no_delta_kernel(node)
    read_only = minio_user(started_cluster, ["s3:GetBucketLocation", "s3:ListBucket", "s3:GetObject"])
    with read_only as (read_only_key, read_only_secret), vended_s3_delta_table(started_cluster, use_v2) as table:
        table_key = table.split("`")[1].replace(".", "/")
        node.query(f"INSERT INTO {table} VALUES (1, 'a')", settings=DELTA_WRITE_SETTINGS)

        def delta_log():
            objects = started_cluster.minio_client.list_objects(
                started_cluster.minio_bucket, prefix=f"{table_key}/_delta_log/", recursive=True
            )
            return sorted(obj.object_name for obj in objects)

        commits = delta_log()

        vend_credentials(node, read_only_key, read_only_secret)
        try:
            assert node.query(f"SELECT count() FROM {table}") == "1\n"
            error = node.query_and_get_error(
                f"INSERT INTO {table} VALUES (2, 'b')", settings=DELTA_WRITE_SETTINGS
            )
        finally:
            vend_credentials(node)
        assert "Access Denied" in error or "AccessDenied" in error, error
        assert read_only_secret not in error

        assert node.query(f"SELECT * FROM {table}") == "1\ta\n"
        assert delta_log() == commits


def test_drop_table(started_cluster):
    """`DROP TABLE` removes the table from Unity, not only from ClickHouse."""
    node = started_cluster.instances["node1"]
    skip_if_no_delta_kernel(node)
    schema_name = unique_name("v2_drop")
    table = f"{schema_name}.dropped"
    db_name = unique_name("v2_drop_db")

    uc_api_post(node, "schemas", {"name": schema_name, "catalog_name": CATALOG})
    create_database(node, db_name)

    node.query(
        f"CREATE TABLE {db_name}.`{table}` (id Int32)"
        f" ENGINE = DeltaLakeLocal('/var/lib/clickhouse/user_files/tmp/{table}')",
        settings=DELTA_WRITE_SETTINGS,
    )
    assert show_tables(node, db_name, f"{schema_name}%") == [table]

    # The table is external, so Databricks keeps its data. Asking to delete it is refused.
    assert "SUPPORT_IS_DISABLED" in node.query_and_get_error(
        f"DROP TABLE {db_name}.`{table}`",
        settings={"iceberg_delete_data_on_drop": 1},
    )
    assert show_tables(node, db_name, f"{schema_name}%") == [table]

    node.query(f"DROP TABLE {db_name}.`{table}`")

    assert show_tables(node, db_name, f"{schema_name}%") == []
    assert "UNKNOWN_TABLE" in node.query_and_get_error(
        f"SELECT * FROM {db_name}.`{table}`"
    )


def test_pat_token_authentication(started_cluster):
    node = started_cluster.instances["node1"]
    db_name = unique_name("v2_pat")

    # Succeeds with correct token.
    create_database(node, db_name, url=PROXY_URL, catalog_credential=PAT_TOKEN)
    assert DELTA_TABLE in show_tables(node, db_name, "default%")
    assert_seeded_rows(node, db_name, DELTA_TABLE)

    # Fails with incorrect token.
    bad_db = unique_name("v2_pat_bad")
    create_database(node, bad_db, url=PROXY_URL, catalog_credential="dapi-wrong")
    error = node.query_and_get_error(f"SHOW TABLES FROM {bad_db}")
    assert "401" in error


def test_oauth_token_refresh(started_cluster):
    # Create DB with client ID and secret.
    node = started_cluster.instances["node1"]
    db_name = unique_name("v2_oauth")
    create_database(node, db_name, PROXY_URL, f"{CLIENT_ID}:{CLIENT_SECRET}")
    assert_seeded_rows(node, db_name, DELTA_TABLE)

    # Expire the token.
    proxy_control(node, "expire")

    # Verify refresh.
    assert_seeded_rows(node, db_name, UNIFORM_TABLE)
    assert_seeded_rows(node, db_name, DELTA_TABLE)

    # Verify failure with wrong secret.
    bad_db = unique_name("v2_oauth_bad")
    create_database(node, bad_db, PROXY_URL, f"{CLIENT_ID}:wrong-secret")
    error = node.query_and_get_error(f"SHOW TABLES FROM {bad_db}")
    assert "401" in error


def test_static_token_expiry(started_cluster):
    node = started_cluster.instances["node1"]
    db_name = unique_name("v2_pat_expired")
    create_database(node, db_name, url=PROXY_URL, catalog_credential=PAT_TOKEN)
    assert_seeded_rows(node, db_name, DELTA_TABLE)

    try:
        proxy_control(node, "revoke_pat")

        error = node.query_and_get_error(f"SHOW TABLES FROM {db_name}")
        assert "401" in error
        assert "LOGICAL_ERROR" not in error

        # Ensure server still starts with an expired token.
        node.restart_clickhouse()

        assert db_name in node.query("SHOW DATABASES").split("\n")
        assert f"{V2_SETTING} = 1" in node.query(f"SHOW CREATE DATABASE {db_name}")

        error = node.query_and_get_error(f"SHOW TABLES FROM {db_name}")
        assert "401" in error
        assert "LOGICAL_ERROR" not in error
    finally:
        proxy_control(node, "restore_pat")

    assert_seeded_rows(node, db_name, DELTA_TABLE)


def test_iceberg_rest_denied_keeps_database_usable(started_cluster):
    """No Iceberg REST access: the table is unreadable, the database still works."""
    node = started_cluster.instances["node1"]
    db_name = unique_name("v2_iceberg_rest_denied")
    create_database(node, db_name, url=PROXY_URL, catalog_credential=PAT_TOKEN)

    def column_tables():
        return set(
            node.query(
                f"SELECT DISTINCT table FROM system.columns WHERE database = '{db_name}'",
                settings={"show_data_lake_catalogs_in_system_tables": "1"},
            )
            .strip()
            .split("\n")
        )

    try:
        proxy_control(node, "deny_iceberg_rest")

        # Listing does not touch the REST endpoint, so the table is still shown.
        assert UNIFORM_TABLE in show_tables(node, db_name, "default%")

        # Goes through getTablesIterator.
        tables = column_tables()
        assert DELTA_TABLE in tables
        assert UNIFORM_TABLE not in tables

        error = node.query_and_get_error(f"SELECT * FROM {db_name}.`{UNIFORM_TABLE}`")
        assert "EXTERNAL USE SCHEMA" in error
        assert "External data access is not enabled" in error
        assert "LOGICAL_ERROR" not in error

        assert_seeded_rows(node, db_name, DELTA_TABLE)

        # Not sticky.
        proxy_control(node, "allow_iceberg_rest")
        assert_seeded_rows(node, db_name, UNIFORM_TABLE)
        assert UNIFORM_TABLE in column_tables()

        proxy_control(node, "deny_iceberg_rest")
        node.query(f"DROP DATABASE {db_name}")
    finally:
        proxy_control(node, "allow_iceberg_rest")


def test_no_secrets_leaked(started_cluster):
    """`catalog_credential` must not appear in `SHOW CREATE`, `system.databases`, errors, or logs."""
    node = started_cluster.instances["node1"]
    secrets = [PAT_TOKEN, CLIENT_SECRET]
    query_ids = []

    def query(sql, **kwargs):
        qid = uuid.uuid4().hex
        query_ids.append(qid)
        return node.query(sql, query_id=qid, **kwargs)

    def create(db_name, credential):
        query(
            f"""
CREATE DATABASE {db_name} ENGINE = DataLakeCatalog('{PROXY_URL}')
SETTINGS warehouse = '{CATALOG}', catalog_type = 'unity', {V2_SETTING} = 1,
         vended_credentials = false, catalog_credential = '{credential}'
            """,
            settings={GATE_SETTING: "1"},
        )

    databases = {
        unique_name("v2_leak_pat"): PAT_TOKEN,
        unique_name("v2_leak_oauth"): f"{CLIENT_ID}:{CLIENT_SECRET}",
    }
    for db_name, credential in databases.items():
        create(db_name, credential)
        # Exercise the auth path so that the token exchange gets logged.
        assert DELTA_TABLE in query(f"SHOW TABLES FROM {db_name}")

        show_create = query(f"SHOW CREATE DATABASE {db_name}")
        assert "[HIDDEN]" in show_create
        for secret in secrets:
            assert secret not in show_create

        engine_full_sql = (
            f"SELECT engine_full FROM system.databases WHERE name = '{db_name}'"
        )
        engine_full = query(engine_full_sql)
        assert "[HIDDEN]" in engine_full
        for secret in secrets:
            assert secret not in engine_full
        # The value is masked, not dropped.
        assert credential in node.query(
            engine_full_sql,
            settings={"format_display_secrets_in_show_and_select": 1},
        )

    # Authentication errors must not echo the credential.
    wrong_pat = f"dapi-wrong-{uuid.uuid4().hex}"
    wrong_secret = f"wrong-secret-{uuid.uuid4().hex}"
    for credential, secret in [
        (wrong_pat, wrong_pat),
        (f"{CLIENT_ID}:{wrong_secret}", wrong_secret),
    ]:
        bad_db = unique_name("v2_leak_bad")
        create(bad_db, credential)
        error = node.query_and_get_error(f"SHOW TABLES FROM {bad_db}")
        assert "401" in error
        assert secret not in error
        node.query(f"DROP DATABASE {bad_db}")

    node.query("SYSTEM FLUSH LOGS system.query_log")
    node.query("SYSTEM FLUSH LOGS system.text_log")

    id_list = ", ".join(f"'{qid}'" for qid in query_ids)
    # All queries must be in the log, otherwise the checks below pass on empty output.
    assert node.query(
        f"SELECT count() FROM system.query_log WHERE query_id IN ({id_list}) AND type = 'QueryFinish'"
    ).strip() == str(len(query_ids))
    logged_queries = node.query(
        f"SELECT query FROM system.query_log WHERE query_id IN ({id_list})"
    )
    for secret in secrets:
        assert secret not in logged_queries

    text_log_rows = node.query(f"""
SELECT message, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10
FROM system.text_log
WHERE query_id IN ({id_list})
FORMAT JSONEachRow
""").strip()
    assert text_log_rows
    for line in text_log_rows.split("\n"):
        for val in json.loads(line).values():
            if isinstance(val, str):
                for secret in secrets:
                    assert secret not in val

    for db_name in databases:
        node.query(f"DROP DATABASE {db_name}")
