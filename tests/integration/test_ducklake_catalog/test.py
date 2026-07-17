import os
import tarfile

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check

# Fixtures in data/ were generated once with DuckDB 1.5.x + the ducklake extension
# (ATTACH 'ducklake:sqlite:catalog.db' with DATA_INLINING_ROW_LIMIT 0), then data_path
# was rewritten to the user_files location. No DuckDB dependency is needed at test time.
# catalog.sql is a PostgreSQL dump of the same catalog (catalog.db is used for sqlite).

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)

cluster.base_cmd.extend(
    ["--file", os.path.join(get_docker_compose_path(), "docker_compose_postgres.yml")]
)

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "data")


def create_sqlite_db():
    node.query("DROP DATABASE IF EXISTS ducklake_sqlite SYNC")
    node.query(
        "CREATE DATABASE ducklake_sqlite ENGINE = DataLakeCatalog('ducklake')"
        " SETTINGS catalog_type = 'ducklake', ducklake_backend = 'sqlite',"
        " ducklake_connection_string = 'catalog.db';",
        settings={"allow_experimental_database_ducklake_catalog": 1},
    )


def create_postgres_db():
    node.query("DROP DATABASE IF EXISTS ducklake_pg SYNC")
    # the postgres compose fixture uses trust auth, so no password is needed (and the
    # test-only SensitiveDataMasker rule forbids one in query text)
    node.query(
        "CREATE DATABASE ducklake_pg ENGINE = DataLakeCatalog('ducklake')"
        " SETTINGS catalog_type = 'ducklake', ducklake_backend = 'postgres',"
        " ducklake_connection_string = 'host=postgres1 port=5432 dbname=postgres user=postgres';",
        settings={"allow_experimental_database_ducklake_catalog": 1},
    )


def copy_dir_to_container(instance, src_dir, dest_dir):
    """copy_file_to_container handles files only; ship a tarball instead."""
    tar_path = os.path.join(
        cluster.instances_dir, instance.name, os.path.basename(src_dir) + ".tar.gz"
    )
    os.makedirs(os.path.dirname(tar_path), exist_ok=True)
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(src_dir, arcname=os.path.basename(src_dir))
    instance.copy_file_to_container(tar_path, "/tmp/data.tar.gz")
    instance.exec_in_container(
        ["bash", "-c", f"mkdir -p {os.path.dirname(dest_dir)} && tar -xzf /tmp/data.tar.gz -C {os.path.dirname(dest_dir)}"]
    )


@pytest.fixture(scope="module")
def started_cluster():
    cluster.start()
    try:
        copy_dir_to_container(
            node,
            os.path.join(FIXTURES_DIR, "ducklake_data"),
            "/var/lib/clickhouse/user_files/ducklake_data",
        )
        copy_dir_to_container(
            node,
            os.path.join(FIXTURES_DIR, "ducklake_fail_data"),
            "/var/lib/clickhouse/user_files/ducklake_fail_data",
        )
        # the .db files are too large for copy_file_to_container (argv limit), tar them
        tar_path = os.path.join(cluster.instances_dir, node.name, "catalogs.tar.gz")
        os.makedirs(os.path.dirname(tar_path), exist_ok=True)
        with tarfile.open(tar_path, "w:gz") as tar:
            for db_file in ("catalog.db", "catalog_inlined.db", "catalog_badversion.db"):
                tar.add(os.path.join(FIXTURES_DIR, db_file), arcname=db_file)
        node.copy_file_to_container(tar_path, "/tmp/catalogs.tar.gz")
        node.exec_in_container(
            ["bash", "-c", "tar -xzf /tmp/catalogs.tar.gz -C /var/lib/clickhouse/user_files"]
        )

        postgres_container_id = cluster.get_instance_docker_id("postgres1")
        cluster.copy_file_to_container(
            postgres_container_id,
            os.path.join(FIXTURES_DIR, "catalog.sql"),
            "/tmp/catalog.sql",
        )
        run_and_check(
            [
                f"docker exec {postgres_container_id} psql -U postgres -d postgres -f /tmp/catalog.sql"
            ],
            shell=True,
        )
        yield cluster
    finally:
        cluster.shutdown()


def run_checks(database):
    assert node.query("SHOW TABLES", database=database) == (
        "main.evolved\n"
        "main.nested\n"
        "main.plain\n"
        "main.types\n"
        "main.with_deletes\n"
    )

    assert (
        node.query("SELECT * FROM `main.plain` ORDER BY id", database=database)
        == "1\ta\n2\tb\n3\tc\n"
    )

    assert (
        node.query("SELECT * FROM `main.nested` ORDER BY id", database=database)
        == "1\t(1,'u')\t[1,2]\t{'a':1}\n2\t(2,'v')\t[3]\t{'b':2}\n"
    )

    # positional deletes: rows 2 and 3 are deleted
    assert (
        node.query("SELECT * FROM `main.with_deletes` ORDER BY id", database=database)
        == "1\ta\n4\td\n"
    )

    # schema evolution: added column is filled with defaults for old files,
    # renamed-then-dropped column is gone entirely
    assert (
        node.query("DESC `main.evolved`", database=database)
        == "id\tNullable(Int32)\t\t\t\t\t\nextra\tNullable(Float64)\t\t\t\t\t\n"
    )
    assert (
        node.query("SELECT * FROM `main.evolved` ORDER BY id", database=database)
        == "1\t\\N\n2\t\\N\n3\t1.5\n4\t2.5\n5\t3.5\n"
    )

    assert (
        node.query(
            "SELECT b, i8, i16, i32, i64, h, u8, u16, u32, u64, f32, f64, d, vc, bl, dt, tm, ts, tstz, u "
            "FROM `main.types`",
            database=database,
        )
        == "1\t-1\t-2\t-3\t-4\t12345\t1\t2\t3\t4\t1.5\t2.5\t12.34\tstr\tblobdata\t"
        "2024-01-15\t10:30:00\t2024-01-15 10:30:00.000000\t2024-01-15 10:30:00.000000\t"
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n"
    )


def test_ducklake_sqlite(started_cluster):
    create_sqlite_db()
    run_checks("ducklake_sqlite")


def test_ducklake_postgres(started_cluster):
    create_postgres_db()
    run_checks("ducklake_pg")


def test_requires_experimental_setting(started_cluster):
    node.query("DROP DATABASE IF EXISTS ducklake_no_setting SYNC")
    with pytest.raises(QueryRuntimeException, match="allow_experimental_database_ducklake_catalog"):
        node.query(
            "CREATE DATABASE ducklake_no_setting ENGINE = DataLakeCatalog('ducklake')"
            " SETTINGS catalog_type = 'ducklake', ducklake_backend = 'sqlite',"
            " ducklake_connection_string = 'catalog.db';"
        )


def test_requires_connection_string(started_cluster):
    node.query("DROP DATABASE IF EXISTS ducklake_no_conn SYNC")
    with pytest.raises(QueryRuntimeException, match="ducklake_connection_string"):
        node.query(
            "CREATE DATABASE ducklake_no_conn ENGINE = DataLakeCatalog('ducklake')"
            " SETTINGS catalog_type = 'ducklake', ducklake_backend = 'sqlite';",
            settings={"allow_experimental_database_ducklake_catalog": 1},
        )


def test_inlined_data_rejected(started_cluster):
    node.query("DROP DATABASE IF EXISTS ducklake_inlined SYNC")
    node.query(
        "CREATE DATABASE ducklake_inlined ENGINE = DataLakeCatalog('ducklake')"
        " SETTINGS catalog_type = 'ducklake', ducklake_backend = 'sqlite',"
        " ducklake_connection_string = 'catalog_inlined.db';",
        settings={"allow_experimental_database_ducklake_catalog": 1},
    )
    with pytest.raises(QueryRuntimeException, match="inlined data"):
        node.query("SELECT * FROM `main.inl`", database="ducklake_inlined")


def test_unsupported_catalog_version(started_cluster):
    node.query("DROP DATABASE IF EXISTS ducklake_badversion SYNC")
    with pytest.raises(QueryRuntimeException, match="schema version"):
        node.query(
            "CREATE DATABASE ducklake_badversion ENGINE = DataLakeCatalog('ducklake')"
            " SETTINGS catalog_type = 'ducklake', ducklake_backend = 'sqlite',"
            " ducklake_connection_string = 'catalog_badversion.db';",
            settings={"allow_experimental_database_ducklake_catalog": 1},
        )
