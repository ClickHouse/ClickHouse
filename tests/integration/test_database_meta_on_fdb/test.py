import pytest
from helpers.cluster import ClickHouseCluster
from pathlib import Path
from textwrap import dedent

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    'node',
    main_configs=["configs/foundationdb.xml", "configs/named_collections.xml"],
    with_foundationdb=True,
    stay_alive=True
)

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start(destroy_dirs=True)
        yield cluster

    finally:
        cluster.shutdown()


def test_basic_ddl_operations():
    assert node.query("SELECT count() FROM system.databases WHERE name = 'default'").strip() == "1"

    db_name = "test_basic_ddl_ops"
    node.query(f"CREATE DATABASE {db_name}")
    node.query(f"DETACH DATABASE {db_name}")
    node.query(f"ATTACH DATABASE {db_name}")
    node.query(f"RENAME DATABASE {db_name} to {db_name}2")
    node.query(f"DROP DATABASE {db_name}2")

def test_create_unsupport_database():
    db_name = "test_create_unsupport_database"
    assert "Unsupported database engine" in node.query_and_get_error(f"CREATE DATABASE {db_name} ENGINE = Atomic")
    assert "Unknown database engine" in node.query_and_get_error(f"CREATE DATABASE {db_name} ENGINE = AAtomic")

def test_attach_not_exists_database():
    db_name = "test_attach_not_exists_database"
    assert "Cannot find database" in node.query_and_get_error(f"ATTACH DATABASE {db_name}")

def test_should_not_overwrite_detached_database():
    db_name = "test_overwrite_detached"
    node.query(f"CREATE DATABASE {db_name}")
    node.query(f"DETACH DATABASE {db_name}")
    assert "exists" in node.query_and_get_error(f"CREATE DATABASE {db_name}")

def test_database_should_be_persisted():
    db_name = "test_presist"

    node.query(dedent(f"""\
        CREATE DATABASE {db_name}
        COMMENT 'comment'
    """))
    uuid = node.query(f"SELECT uuid FROM system.databases WHERE name = '{db_name}'")
    node.restart_clickhouse()
    assert node.query(f"SHOW CREATE DATABASE {db_name}") == \
        f"CREATE DATABASE {db_name}\\n" \
        "ENGINE = OnFDB\\n" \
        "COMMENT \\'comment\\'\n"
    assert node.query(f"SELECT uuid FROM system.databases WHERE name = '{db_name}'") == uuid

def test_show_create_database():
    db_name = "test_show_create"
    node.query(f"CREATE DATABASE {db_name};")
    assert node.query(f"SHOW CREATE DATABASE {db_name}") == f"CREATE DATABASE {db_name}\\nENGINE = OnFDB\n"

def test_rename_should_be_persisted():
    db_name = "test/presist_rename"
    db_name_new = "test/presist_rename_new"

    node.query(dedent(f"""\
        CREATE DATABASE `{db_name}`;
        RENAME DATABASE `{db_name}` TO `{db_name_new}`
    """))

    node.restart_clickhouse()

    assert node.query(f"SHOW CREATE DATABASE `{db_name_new}`") == f"CREATE DATABASE `{db_name_new}`\\nENGINE = OnFDB\n"
    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}'").strip() == "0"
    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name_new}'").strip() == "1"

def test_drop_should_be_persisted():
    db_name = "test_presist_drop"

    node.query(dedent(f"""\
        CREATE DATABASE {db_name};
        DROP DATABASE {db_name};
    """))

    node.restart_clickhouse()

    assert node.query(f"SELECT count() FROM system.databases WHERE name = '{db_name}'").strip() == "0"

def test_rename_database_with_tables():
    db_name = "test_rename_database_with_tables"
    db_name2 = "test_rename_database_with_tables2"

    node.query(dedent(f"""\
        CREATE DATABASE {db_name};
        CREATE TABLE {db_name}.b (a int) ENGINE MergeTree() ORDER BY a;
        CREATE TABLE {db_name}.c (a int) ENGINE MergeTree() ORDER BY a;
        RENAME DATABASE {db_name} TO {db_name2};
    """))

    node.restart_clickhouse()

    assert node.query(f"select name from system.tables where database = '{db_name2}'") == "b\nc\n"
