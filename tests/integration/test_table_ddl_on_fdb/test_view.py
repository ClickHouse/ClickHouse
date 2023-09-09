import pytest
from helpers.cluster import ClickHouseCluster
from pathlib import Path
from textwrap import dedent

@pytest.fixture(scope="module")
def started_cluster(request):
    try:
        cluster = ClickHouseCluster(__file__, name = "view")
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

def test_view_basic_ddl(started_cluster):
    db_name = "test_basic_view_ops"
    tb_name = "test_basic_view_ops_tb_name"
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

    view_name = "test_basic_view_ops"
    node.query(dedent(f"""\
        CREATE VIEW {db_name}.{view_name}
        AS
        SELECT * FROM {db_name}.{tb_name}
    """))
    node.query(f"DETACH VIEW {db_name}.{view_name}")
    node.query(f"ATTACH TABLE {db_name}.{view_name}") #ATTACH VIEW doesn't exists
    #RENAME VIEW doesn't exists, view can be renamed by RENAME TABLE
    node.query(f"RENAME TABLE {db_name}.{view_name} TO {db_name}.{view_name}2")
    node.query(f"DROP VIEW {db_name}.{view_name}2")

    materialized_view_name = "test_materialized_view"
    node.query(dedent(f"""\
        CREATE MATERIALIZED VIEW {db_name}.{materialized_view_name}
        (
            `n` UInt64,
            `m` UInt64
        )
        ENGINE = MergeTree
        ORDER BY n
        AS SELECT * FROM {db_name}.{tb_name}
    """) )

    live_view_name = "test_live_view"
    node.query(dedent(f"""\
        CREATE LIVE VIEW {db_name}.{live_view_name}
        AS SELECT * FROM {db_name}.{tb_name} 
        SETTINGS allow_experimental_live_view=True
    """) )
    
def test_show_create_view(started_cluster):
    db_name = "test_show_create_view"
    tb_name = "test_show_create_view_tb_name"
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

    view_name = "test_show_create_view"
    node.query(dedent(f"""\
        CREATE VIEW {db_name}.{view_name}
        AS
        SELECT * FROM {db_name}.{tb_name}
    """))
    assert node.query(f"SHOW CREATE VIEW {db_name}.{view_name}") == f"CREATE VIEW {db_name}.{view_name}\\n(\\n    `n` UInt64,\\n    `m` UInt64\\n) AS\\nSELECT *\\nFROM {db_name}.{tb_name}\n"
    
def test_create_view_should_be_persisted(started_cluster):
    db_name = "test_create_view_should_be_persisted"
    tb_name = "test_create_view_should_be_persisted_tb_name"
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

    view_name = "test_create_view_should_be_persisted"
    node.query(dedent(f"""\
        CREATE VIEW {db_name}.{view_name}
        AS
        SELECT * FROM {db_name}.{tb_name}
    """))

    node.restart_clickhouse()
    assert node.query(f"SELECT count() FROM system.tables WHERE database = '{db_name}' and name = '{view_name}'").strip() == "1"


def test_drop_view_should_be_persisted(started_cluster):
    db_name = "test_drop_view_should_be_persisted"
    tb_name = "test_drop_view_should_be_persisted_tb_name"
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

    view_name = "test_drop_view_should_be_persisted"
    node.query(dedent(f"""\
        CREATE VIEW {db_name}.{view_name}
        AS
        SELECT * FROM {db_name}.{tb_name}
    """))
    node.query(f"DROP VIEW {db_name}.{view_name}")
    node.restart_clickhouse()
    assert node.query(f"SELECT count() FROM system.tables WHERE database = '{db_name}' and name = '{view_name}'").strip() == "0"

