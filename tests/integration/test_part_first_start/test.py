import pytest
import os
from helpers.cluster import ClickHouseCluster
from textwrap import dedent

@pytest.fixture(scope="module")
def started_cluster(request):
    try:
        cluster = ClickHouseCluster(__file__)
        node = cluster.add_instance(
            'node',
            with_foundationdb=True,
            stay_alive=True
        )
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

def test_compact_part_first_start(started_cluster):
    db_name = "test_compact_part_first_start"
    tb_name = "test_compact_part_first_start"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name}
        (
            `a` UInt64,
            `b` UInt64
        ) 
        ENGINE = MergeTree
        PARTITION BY a
        ORDER BY b
    """))
    
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,2)")
    node.restart_clickhouse()
    parts_new_info = node.query(f"SELECT * FROM system.parts where database = '{db_name}' and table = '{tb_name}'").strip()
    parts_columns_new_info = node.query(f"SELECT * FROM system.parts_columns where database = '{db_name}' and table = '{tb_name}' order by column").strip()
    
    node.stop_clickhouse()
    with open(os.path.dirname(__file__) + "/configs/foundationdb.xml", "r") as f:
        node.replace_config("/etc/clickhouse-server/config.d/foundationdb.xml", f.read())
    # First start
    node.start_clickhouse()
    
    parts_old_info = node.query(f"SELECT * FROM system.parts where database = '{db_name}' and table = '{tb_name}'").strip()
    parts_columns_old_info = node.query(f"SELECT * FROM system.parts_columns where database = '{db_name}' and table = '{tb_name}' order by column").strip()
    
    assert parts_old_info == parts_new_info
    assert parts_columns_old_info == parts_columns_new_info
    

     
def test_wide_part_start_with_parts(started_cluster):
    db_name = "test_wide_part_start_with_parts"
    tb_name = "test_wide_part_start_with_parts"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name}
        (
            `a` UInt64,
            `b` UInt64
        ) 
        ENGINE = MergeTree
        PARTITION BY a
        ORDER BY b
        SETTINGS min_rows_for_wide_part = 2,min_bytes_for_wide_part = 2
    """) )
    
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,2),(1,3)")
    node.restart_clickhouse()
    parts_new_info = node.query(f"SELECT * FROM system.parts where database = '{db_name}' and table = '{tb_name}'").strip()
    parts_columns_new_info = node.query(f"SELECT * FROM system.parts_columns where database = '{db_name}' and table = '{tb_name}' order by column").strip()
    node.stop_clickhouse()
    with open(os.path.dirname(__file__) + "/configs/foundationdb.xml", "r") as f:
        node.replace_config("/etc/clickhouse-server/config.d/foundationdb.xml", f.read())
    # First start with FDB
    node.start_clickhouse()
    
    parts_old_info = node.query(f"SELECT * FROM system.parts where database = '{db_name}' and table = '{tb_name}'").strip()
    parts_columns_old_info = node.query(f"SELECT * FROM system.parts_columns where database = '{db_name}' and table = '{tb_name}' order by column").strip()
    
    assert parts_old_info == parts_new_info
    assert parts_columns_old_info == parts_columns_new_info


