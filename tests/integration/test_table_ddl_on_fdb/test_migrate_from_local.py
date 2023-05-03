import os.path
import pytest
from helpers.cluster import ClickHouseCluster
from pathlib import Path
from textwrap import dedent

cluster = ClickHouseCluster(__file__, name="migrate")
node = cluster.add_instance(
    'node',
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


def test_migrate_from_local():
    db_name = "test_migrate"
    tb_name = "test_migrate"
    tb_detached_name = "test_detached_permanently"
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

    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_detached_name} 
        (   
            `n` UInt64,
            `m` UInt64
        ) 
        ENGINE = MergeTree 
        ORDER BY n 
        PRIMARY KEY (n)
        SETTINGS index_granularity = 8192
    """) )
    assert node.query(f"SELECT count() FROM system.tables WHERE database = '{db_name}' and name = '{tb_detached_name}'").strip() == "1"

    node.query(f"DETACH TABLE {db_name}.{tb_detached_name} PERMANENTLY")
    assert node.query(f"SELECT count() FROM system.tables WHERE database = '{db_name}' and name = '{tb_detached_name}'").strip() == "0"

    node.stop_clickhouse()
    with open(os.path.dirname(__file__) + "/configs/foundationdb.xml", "r") as f:
        node.replace_config("/etc/clickhouse-server/config.d/foundationdb.xml", f.read())

    node.start_clickhouse()
    assert node.query(f"SELECT count() FROM system.tables WHERE database = '{db_name}' and name = '{tb_name}'").strip() == "1"
    # test detached permanently tables loaded
    node.query(f"ATTACH TABLE {db_name}.{tb_detached_name}")
    assert node.query(f"SELECT count() FROM system.tables WHERE database = '{db_name}' and name = '{tb_name}'").strip() == "1"

