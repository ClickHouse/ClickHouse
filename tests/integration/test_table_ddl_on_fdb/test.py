import pytest
from helpers.cluster import ClickHouseCluster
from pathlib import Path
from textwrap import dedent

@pytest.fixture(scope="module")
def started_cluster(request):
    try:
        cluster = ClickHouseCluster(__file__)
        node = cluster.add_instance(
            'node',
            main_configs=["configs/foundationdb.xml"],
            with_foundationdb=True,
            stay_alive=True
        )
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

def test_basic_ddl_operations(started_cluster):
    db_name = "test_basic_ddl_ops"
    tb_name = "test_basic_ddl_ops"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )

    node.query(f"ALTER TABLE {db_name}.{tb_name} ADD COLUMN Added1 UInt32 FIRST")
    node.query(f"DETACH TABLE {db_name}.{tb_name}")
    node.query(f"ATTACH TABLE {db_name}.{tb_name}")
    node.query(f"RENAME TABLE {db_name}.{tb_name} TO {db_name}.{tb_name}2")
    node.query(f"DROP TABLE {db_name}.{tb_name}2")

def test_detached_table_flag_should_be_persisted(started_cluster):
    db_name = "test_detached_table_flag"
    tb_name = "test_detached_table_flag"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )
    node.query(f"DETACH TABLE {db_name}.{tb_name} PERMANENTLY")

    node.restart_clickhouse()
    assert node.query(f"SELECT count() FROM system.tables WHERE database = '{db_name}' and name = '{tb_name}'").strip() == "0"

    node.query(f"ATTACH TABLE {db_name}.{tb_name}")
    assert node.query(f"SELECT count() FROM system.tables WHERE database = '{db_name}' and name = '{tb_name}'").strip() == "1"

def test_create_dropped_table(started_cluster):
    db_name = "test_create_dropped_table"
    tb_name = "test_create_dropped_table"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )
    assert node.query(f"SELECT count() FROM system.tables WHERE database = '{db_name}' and name = '{tb_name}'").strip() == "1"

    node.query(f"DROP TABLE {db_name}.{tb_name}")

    assert node.query(f"SELECT count() FROM system.tables WHERE database = '{db_name}' and name = '{tb_name}'").strip() == "0"

    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )
    assert node.query(f"SELECT count() FROM system.tables WHERE database = '{db_name}' and name = '{tb_name}'").strip() == "1"


def test_drop_same_table(started_cluster):
    db_name = "test_drop_same_table"
    tb_name = "test_drop_same_table"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )
    node.query(f"DROP TABLE {db_name}.{tb_name}")

    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )
    node.query(f"DROP TABLE {db_name}.{tb_name}")

def test_rename_should_be_persisted(started_cluster):
    db_name = "test_persist_rename"
    tb_name = "test_persist_rename"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")

    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )
    node.query(f"RENAME TABLE {db_name}.{tb_name} to {db_name}.{tb_name}2")


    node.restart_clickhouse()

    assert node.query(f"SELECT count() FROM system.tables WHERE database = '{db_name}' and name = '{tb_name}'").strip() == "0"
    assert node.query(f"SELECT count() FROM system.tables WHERE database = '{db_name}' and name = '{tb_name}2'").strip() == "1"

def test_alter_should_be_persisted(started_cluster):
    db_name = "test_persist_alter"
    tb_name = "test_persist_alter"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")

    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )

    node.query(f"ALTER TABLE {db_name}.{tb_name} ADD COLUMN Added1 UInt32 FIRST")

    node.restart_clickhouse()

    assert node.query(f"SHOW CREATE TABLE {db_name}.{tb_name}") == f"CREATE TABLE test_persist_alter.test_persist_alter\\n(\\n    `Added1` UInt32,\\n    `n` UInt64,\\n    `m` UInt64\\n)\\nENGINE = MergeTree\\nPRIMARY KEY n\\nORDER BY n\\nSETTINGS index_granularity = 8192\n"

def test_drop_should_be_persisted(started_cluster):
    db_name = "test_persist_drop"
    tb_name = "test_persist_drop"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")

    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )
    node.query(f"DROP TABLE {db_name}.{tb_name}")


    node.restart_clickhouse()

    assert node.query(f"SELECT count() FROM system.tables WHERE database = '{db_name}' and name = '{tb_name}'").strip() == "0"

def test_show_create_table(started_cluster):
    db_name = "test_show_create"
    tb_name = "test_show_create"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )

    assert node.query(f"SHOW CREATE TABLE {db_name}.{tb_name}") == f"CREATE TABLE test_show_create.test_show_create\\n(\\n    `n` UInt64,\\n    `m` UInt64\\n)\\nENGINE = MergeTree\\nPRIMARY KEY n\\nORDER BY n\\nSETTINGS index_granularity = 8192\n"

def test_detach_not_exists_table(started_cluster):
    db_name = "test_detach_not_exists_table"
    tb_name = "test_detach_not_exists_table"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")


    assert f"Table {db_name}.{tb_name} doesn't exist" in node.query_and_get_error(f"DETACH TABLE {db_name}.{tb_name}")

def test_attach_not_exists_table(started_cluster):
    db_name = "test_attach_not_exists_table"
    tb_name = "test_attach_not_exists_table"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")

    assert f"Table `{tb_name}` doesn't exist" in node.query_and_get_error(f"ATTACH TABLE {db_name}.{tb_name}")

def test_exchange_ddl(started_cluster):
    db_name = "test_exchange_ddl"
    tb_name = "test_exchange_ddl"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )
    node.query(f"CREATE DATABASE {db_name}_exchange")
    node.query(dedent(f"""\
        CREATE TABLE {db_name}_exchange.{tb_name}_exchange
        (
            `n_exchange` UInt64,
            `m_exchange` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n_exchange
        PRIMARY KEY (n_exchange)
        SETTINGS index_granularity = 8192
    """) )
    node.query(f"EXCHANGE TABLES {db_name}.{tb_name} AND {db_name}_exchange.{tb_name}_exchange")

    assert node.query(f"SHOW CREATE TABLE {db_name}.{tb_name}") == f"CREATE TABLE test_exchange_ddl.test_exchange_ddl\\n(\\n    `n_exchange` UInt64,\\n    `m_exchange` UInt64\\n)\\nENGINE = MergeTree\\nPRIMARY KEY n_exchange\\nORDER BY n_exchange\\nSETTINGS index_granularity = 8192\n"
    assert node.query(f"SHOW CREATE TABLE {db_name}_exchange.{tb_name}_exchange") == f"CREATE TABLE test_exchange_ddl_exchange.test_exchange_ddl_exchange\\n(\\n    `n` UInt64,\\n    `m` UInt64\\n)\\nENGINE = MergeTree\\nPRIMARY KEY n\\nORDER BY n\\nSETTINGS index_granularity = 8192\n"
