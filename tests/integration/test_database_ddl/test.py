import pytest
import os
from helpers.cluster import ClickHouseCluster
from pathlib import Path
from textwrap import dedent

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    'node',
    # Only MaterializedPostgreSQL database engine support alter, so postgres is required.
    main_configs=["configs/named_collections.xml"],
    with_postgres=True,
    stay_alive=True
)
metadata_dir = Path(node.path) / "database/metadata"


def prepare_postgres():
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    conn_string = f"host={cluster.postgres_ip} port={cluster.postgres_port} user='postgres' password='mysecretpassword'"
    conn = psycopg2.connect(conn_string)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    conn.autocommit = True

    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id Integer NOT NULL, value Integer, PRIMARY KEY (id))")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start(destroy_dirs=True)

        prepare_postgres()
        yield cluster

    finally:
        cluster.shutdown()


def test_default_database_created():
    assert node.query("SELECT count() FROM system.databases WHERE name = 'default'").strip() == "1"


def test_create_database():
    db_name = "test_create"

    node.query(f"CREATE DATABASE {db_name}")
    db_uuid = node.query(f"SELECT uuid FROM system.databases WHERE name = '{db_name}'").strip()
    assert db_uuid != ""
    with open(metadata_dir / f"{db_name}.sql") as f:
        # Database name should be _ if database has uuid
        assert f.read() == dedent(f"""\
            ATTACH DATABASE _ UUID '{db_uuid}'
            ENGINE = Atomic
        """)


def test_create_database_no_uuid():
    db_name = "test_create_no_uuid"

    node.query(f"CREATE DATABASE {db_name} ENGINE = Ordinary")
    with open(metadata_dir / f"{db_name}.sql") as f:
        assert f.read() == dedent(f"""\
            ATTACH DATABASE {db_name}
            ENGINE = Ordinary
        """)


def test_create_database_failed():
    db_name = "test_create_database_failed"

    assert "mysqlxx::ConnectionFailed" in node.query_and_get_error(dedent(f"""\
        SET allow_experimental_database_materialized_mysql = True;
        CREATE DATABASE {db_name} ENGINE = MaterializedMySQL('localhost:3306', 'db', 'user', 'pass')
    """))
    assert not (metadata_dir / f"{db_name}.sql").exists()


def test_drop_database():
    db_name = "test_drop"

    node.query(f"CREATE DATABASE {db_name}")
    node.query(f"DROP DATABASE {db_name}")
    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}'").strip() == "0"
    assert not os.path.exists(metadata_dir / f"{db_name}.sql")


def test_rename_database():
    db_name1 = "test_rename"
    db_name2 = "test_rename2"

    node.query(f"CREATE DATABASE {db_name1}")
    with open(metadata_dir / f"{db_name1}.sql") as f:
        ddl = f.read()

    node.query(f"RENAME DATABASE {db_name1} TO {db_name2}")

    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name1}'").strip() == "0"
    assert not os.path.exists(metadata_dir / f"{db_name1}.sql")

    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name2}'").strip() == "1"
    assert os.path.exists(metadata_dir / f"{db_name2}.sql")
    with open(metadata_dir / f"{db_name2}.sql") as f:
        assert f.read() == ddl


def test_alter_database():
    db_name = "test_alter"

    node.query(dedent(f"""\
        SET allow_experimental_database_materialized_postgresql = True;
        CREATE DATABASE {db_name} ENGINE = MaterializedPostgreSQL(postgres) SETTINGS materialized_postgresql_allow_automatic_update = True
    """))

    db_uuid = node.query(f"SELECT uuid FROM system.databases WHERE name = '{db_name}'").strip()
    assert db_uuid != ""

    with open(metadata_dir / f"{db_name}.sql") as f:
        assert f.read() == dedent(f"""\
            ATTACH DATABASE _ UUID '{db_uuid}'
            ENGINE = MaterializedPostgreSQL(postgres)
            SETTINGS materialized_postgresql_allow_automatic_update = 1
        """)

    node.query(f"ALTER DATABASE {db_name} MODIFY SETTING materialized_postgresql_allow_automatic_update = False")
    with open(metadata_dir / f"{db_name}.sql") as f:
        assert f.read() == dedent(f"""\
            ATTACH DATABASE {db_name} UUID '{db_uuid}'
            ENGINE = MaterializedPostgreSQL(postgres)
            SETTINGS materialized_postgresql_allow_automatic_update = 0
        """)


def test_load_database():
    db_name = "test_load"

    node.query(f"CREATE DATABASE {db_name}")
    node.restart_clickhouse()

    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}'").strip() == "1"


def test_load_old_style_ordinary_database():
    db_name = "test_load_old_ordinary"

    assert not (metadata_dir / f"{db_name}.sql").exists()
    (metadata_dir / db_name).mkdir()

    node.restart_clickhouse()
    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}'").strip() == "1"
    with open(metadata_dir / f"{db_name}.sql") as f:
        assert f.read() == dedent(f"""\
            ATTACH DATABASE {db_name}
            ENGINE = Ordinary
        """)

def test_show_create_database():
    db_name = "test_show_create"
    node.query(f"CREATE DATABASE {db_name};")
    assert node.query(f"SHOW CREATE DATABASE {db_name}") == f"CREATE DATABASE {db_name}\\nENGINE = Atomic\n"
