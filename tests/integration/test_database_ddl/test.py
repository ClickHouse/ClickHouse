import pytest
import os
from helpers.cluster import ClickHouseCluster
from pathlib import Path
from textwrap import dedent

@pytest.fixture(scope="module")
def started_cluster(request):
    try:
        cluster = ClickHouseCluster(__file__)
        node = cluster.add_instance(
            "node",
            stay_alive=True,
            main_configs=["configs/named_collections.xml"],
            with_postgres=True,
        )
        cluster.start()
        import psycopg2
        from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

        conn_string = f"host={cluster.postgres_ip} port={cluster.postgres_port} user='postgres' password='mysecretpassword'"
        conn = psycopg2.connect(conn_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        conn.autocommit = True

        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id Integer NOT NULL, value Integer, PRIMARY KEY (id))")
        yield cluster
    finally:
        cluster.shutdown()

def test_default_database_created(started_cluster):
    node = started_cluster.instances["node"]
    assert node.query("SELECT count() FROM system.databases WHERE name = 'default'").strip() == "1"

def test_create_database(started_cluster):
    db_name = "test_create"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")
    db_uuid = node.query(f"SELECT uuid FROM system.databases WHERE name = '{db_name}'").strip()
    assert db_uuid != ""
    metadata_dir = Path(node.path) / "database/metadata"
    with open(metadata_dir / f"{db_name}.sql") as f:
        # Database name should be _ if database has uuid
        assert f.read() == dedent(f"""\
            ATTACH DATABASE _ UUID '{db_uuid}'
            ENGINE = Atomic
        """)

def test_create_database_no_uuid(started_cluster):
    db_name = "test_create_no_uuid"
    node = started_cluster.instances["node"]
    node.query(
        dedent(f"""\
        SET allow_deprecated_database_ordinary = True;
        CREATE DATABASE {db_name} ENGINE = Ordinary"""))
    metadata_dir = Path(node.path) / "database/metadata"
    with open(metadata_dir / f"{db_name}.sql") as f:
        assert f.read() == dedent(f"""\
            ATTACH DATABASE {db_name}
            ENGINE = Ordinary
        """)

def test_create_database_failed(started_cluster):
    db_name = "test_create_database_failed"
    node = started_cluster.instances["node"]
    assert "mysqlxx::ConnectionFailed" in node.query_and_get_error(dedent(f"""\
        SET allow_experimental_database_materialized_mysql = True;
        CREATE DATABASE {db_name} ENGINE = MaterializedMySQL('localhost:3306', 'db', 'user', 'pass')
    """))
    metadata_dir = Path(node.path) / "database/metadata"
    assert not (metadata_dir / f"{db_name}.sql").exists()

def test_drop_database(started_cluster):
    db_name = "test_drop"
    node = started_cluster.instances["node"]
    metadata_dir = Path(node.path) / "database/metadata"
    node.query(f"CREATE DATABASE {db_name}")
    node.query(f"DROP DATABASE {db_name}")
    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}'").strip() == "0"
    assert not os.path.exists(metadata_dir / f"{db_name}.sql")

def test_rename_database(started_cluster):
    db_name1 = "test_rename"
    db_name2 = "test_rename2"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name1}")
    metadata_dir = Path(node.path) / "database/metadata"
    with open(metadata_dir / f"{db_name1}.sql") as f:
        ddl = f.read()

    node.query(f"RENAME DATABASE {db_name1} TO {db_name2}")

    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name1}'").strip() == "0"
    assert not os.path.exists(metadata_dir / f"{db_name1}.sql")

    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name2}'").strip() == "1"
    assert os.path.exists(metadata_dir / f"{db_name2}.sql")
    with open(metadata_dir / f"{db_name2}.sql") as f:
        assert f.read() == ddl

# @pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
# def test_alter_database(started_cluster):
#     db_name = "test_alter"
#     node = started_cluster.instances["node"]
#     node.query(dedent(f"""\
#         CREATE DATABASE {db_name} ENGINE = MaterializedPostgreSQL(postgres) SETTINGS materialized_postgresql_allow_automatic_update = True
#     """))

#     db_uuid = node.query(f"SELECT uuid FROM system.databases WHERE name = '{db_name}'").strip()
#     assert db_uuid != ""

#     with open(metadata_dir / f"{db_name}.sql") as f:
#         assert f.read() == dedent(f"""\
#             ATTACH DATABASE _ UUID '{db_uuid}'
#             ENGINE = MaterializedPostgreSQL(postgres)
#             SETTINGS materialized_postgresql_allow_automatic_update = 1
#         """)

#     node.query(f"ALTER DATABASE {db_name} MODIFY SETTING materialized_postgresql_allow_automatic_update = False")
#     with open(metadata_dir / f"{db_name}.sql") as f:
#         assert f.read() == dedent(f"""\
#             ATTACH DATABASE {db_name} UUID '{db_uuid}'
#             ENGINE = MaterializedPostgreSQL(postgres)
#             SETTINGS materialized_postgresql_allow_automatic_update = 0
#         """)

def test_load_database(started_cluster):
    db_name = "test_load"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")
    node.restart_clickhouse()

    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}'").strip() == "1"

# @pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
# def test_load_old_style_ordinary_database(started_cluster):
#     db_name = "test_load_old_ordinary"
#     node = started_cluster.instances["node"]
#     metadata_dir = Path(node.path) / "database/metadata"
#     assert not (metadata_dir / f"{db_name}.sql").exists()
#     (metadata_dir / db_name).mkdir()

#     node.restart_clickhouse()
#     node = started_cluster.instances["node"]
#     assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}'").strip() == "1"
#     with open(metadata_dir / f"{db_name}.sql") as f:
#         assert f.read() == dedent(f"""\
#             ATTACH DATABASE {db_name}
#             ENGINE = Ordinary
#         """)

def test_show_create_database(started_cluster):
    db_name = "test_show_create"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name};")
    assert node.query(f"SHOW CREATE DATABASE {db_name}") == f"CREATE DATABASE {db_name}\\nENGINE = Atomic\n"
