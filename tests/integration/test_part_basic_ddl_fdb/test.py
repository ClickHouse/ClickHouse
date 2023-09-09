import pytest
import os
from helpers.cluster import ClickHouseCluster
from textwrap import dedent
import time

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

def test_basic_delete_part(started_cluster):
    db_name = "test_basic_delete_part"
    tb_name = "test_basic_delete_part"
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
        SETTINGS old_parts_lifetime = 1
    """) )
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,2)")
    assert node.query(f"SELECT count() FROM system.parts where database = '{db_name}' and table = '{tb_name}'").strip() == "1"
    assert node.query(f"SELECT count() FROM system.parts_columns where database = '{db_name}' and table = '{tb_name}'").strip() == "2"
    
    node.query(f"ALTER TABLE {db_name}.{tb_name} DELETE WHERE a = 1")
    
    time.sleep(30)
    assert node.query(f"SELECT count() FROM system.parts where database = '{db_name}' and table = '{tb_name}'").strip() == "0"
    assert node.query(f"SELECT count() FROM system.parts_columns where database = '{db_name}' and table = '{tb_name}'").strip() == "0"
    
def test_attach_part(started_cluster):
    db_name = "test_attach_part"
    tb_name = "test_attach_part"
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
        SETTINGS old_parts_lifetime = 10
    """) )
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,2)")
    table_uuid = node.query(f"SELECT uuid FROM system.tables where database = '{db_name}' and table = '{tb_name}'").strip()
    assert node.query(f"SELECT count() FROM system.parts where database = '{db_name}' and table = '{tb_name}'").strip() == "1"
    assert node.query(f"SELECT count() FROM system.parts_columns where database = '{db_name}' and table = '{tb_name}'").strip() == "2"

    
    part_ck_parts_name = node.query(f"SELECT name FROM system.parts where database = '{db_name}' and table = '{tb_name}'").strip()
    node.query(f"ALTER TABLE {db_name}.{tb_name} DETACH PART '{part_ck_parts_name}'")
    
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'detached_part' and key[1] = '{table_uuid}'").strip() == "1"
    assert node.query(f"SELECT count() FROM system.detached_parts where database = '{db_name}' and table = '{tb_name}'").strip() == "1"

    node.query(f"ALTER TABLE {db_name}.{tb_name} ATTACH PART '{part_ck_parts_name}'")
    time.sleep(30)
    assert node.query(f"SELECT count() FROM system.parts where database = '{db_name}' and table = '{tb_name}'").strip() == "1"
    assert node.query(f"SELECT count() FROM system.parts_columns where database = '{db_name}' and table = '{tb_name}'").strip() == "2"
    part_ck_parts_new_name = node.query(f"SELECT name FROM system.parts where database = '{db_name}' and table = '{tb_name}'").strip()
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid}' and key[2] = '{part_ck_parts_new_name}'").strip() == "1"
    
    
def test_attach_partition(started_cluster):
    db_name = "test_attach_partition"
    tb_name = "test_attach_partition"
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
        SETTINGS old_parts_lifetime = 10
    """) )
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,2)")
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,3)")
    table_uuid = node.query(f"SELECT uuid FROM system.tables where database = '{db_name}' and table = '{tb_name}'").strip()
    assert node.query(f"SELECT count() FROM system.parts where database = '{db_name}' and table = '{tb_name}'").strip() == "2"
    assert node.query(f"SELECT count() FROM system.parts_columns where database = '{db_name}' and table = '{tb_name}'").strip() == "4"
    
    node.query(f"ALTER TABLE {db_name}.{tb_name} DETACH PARTITION '1'")
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'detached_part' and key[1] = '{table_uuid}'").strip() == "2"
    assert node.query(f"SELECT count() FROM system.detached_parts where database = '{db_name}' and table = '{tb_name}'").strip() == "2"
    
    node.query(f"ALTER TABLE {db_name}.{tb_name} ATTACH PARTITION '1'")
    time.sleep(30)
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'detached_part' and key[1] = '{table_uuid}'").strip() == "0"
    assert node.query(f"SELECT count() FROM system.detached_parts where database = '{db_name}' and table = '{tb_name}'").strip() == "0"


def test_part_drop_database(started_cluster):
    db_name = "test_part_drop_database"
    tb_name = "test_part_drop_database"
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
    """) )
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,2)")
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,3)")
    table_uuid = node.query(f"SELECT uuid FROM system.tables where database = '{db_name}' and table = '{tb_name}'").strip()
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid}'").strip() == "2"
    
    node.query(f"drop database {db_name} sync")
    
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid}' ").strip() == "0"
    

def test_part_drop_table(started_cluster):
    db_name = "test_part_drop_table"
    tb_name = "test_part_drop_table"
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
    """) )
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,2)")
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,3)")
    table_uuid = node.query(f"SELECT uuid FROM system.tables where database = '{db_name}' and table = '{tb_name}'").strip()
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid}'").strip() == "2"
    
    node.query(f"drop table {db_name}.{tb_name} sync")
    
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid}'").strip() == "0"
    
def test_drop_detached_part(started_cluster):
    db_name = "test_drop_detached_part"
    tb_name = "test_drop_detached_part"
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
    """) )
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,2)")
    table_uuid = node.query(f"SELECT uuid FROM system.tables where database = '{db_name}' and table = '{tb_name}'").strip()
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid}'").strip() == "1"
    
    part_ck_parts_name = node.query(f"SELECT name FROM system.parts where database = '{db_name}' and table = '{tb_name}'").strip()
    node.query(f"ALTER TABLE {db_name}.{tb_name} DETACH PART '{part_ck_parts_name}'")
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'detached_part' and key[1] = '{table_uuid}'").strip() == "1"
    
    node.query(f"ALTER TABLE {db_name}.{tb_name} DROP DETACHED PART '{part_ck_parts_name}'", settings={"allow_drop_detached": 1})
    
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'detached_part' and key[1] = '{table_uuid}'").strip() == "0"
    
def test_drop_detached_partition(started_cluster):
    db_name = "test_drop_detached_partition"
    tb_name = "test_drop_detached_partition"
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
    """) )
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,2)")
    table_uuid = node.query(f"SELECT uuid FROM system.tables where database = '{db_name}' and table = '{tb_name}'").strip()
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid}'").strip() == "1"
    
    node.query(f"ALTER TABLE {db_name}.{tb_name} DETACH PARTITION '1'")
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'detached_part' and key[1] = '{table_uuid}'").strip() == "1"
    
    node.query(f"ALTER TABLE {db_name}.{tb_name} DROP DETACHED PARTITION '1'", settings={"allow_drop_detached": 1})
    
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'detached_part' and key[1] = '{table_uuid}'").strip() == "0"
    
def test_drop_part(started_cluster):
    db_name = "test_drop_part"
    tb_name = "test_drop_part"
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
        SETTINGS old_parts_lifetime = 10
    """) )
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,2)")
    table_uuid = node.query(f"SELECT uuid FROM system.tables where database = '{db_name}' and table = '{tb_name}'").strip()
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid}'").strip() == "1"
    
    part_ck_parts_name = node.query(f"SELECT name FROM system.parts where database = '{db_name}' and table = '{tb_name}'").strip()
    node.query(f"ALTER TABLE {db_name}.{tb_name} DROP PART '{part_ck_parts_name}'")
    
    time.sleep(30)

    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid}'").strip() == "0"
    
    
def test_drop_partition(started_cluster):
    db_name = "test_drop_partition"
    tb_name = "test_drop_partition"
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
        SETTINGS old_parts_lifetime = 10
    """) )
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,2)")
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,3)")
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,4)")
    table_uuid = node.query(f"SELECT uuid FROM system.tables where database = '{db_name}' and table = '{tb_name}'").strip()
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid}'").strip() == "3"
    
    node.query(f"ALTER TABLE {db_name}.{tb_name} DROP PARTITION '1'")
    time.sleep(30)
    assert node.query(f"SELECT count() FROM system.parts where database = '{db_name}' and table = '{tb_name}'").strip() == "0"
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid}'").strip() == "0"

def test_compact_insert_part(started_cluster):
    db_name = "test_compact_insert_part"
    tb_name = "test_compact_insert_part"
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
    """) )
    
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,2)")
    table_uuid = node.query(f"SELECT uuid FROM system.tables where database = '{db_name}' and table = '{tb_name}'").strip()
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid}'").strip() == "1"

def test_wide_insert_part(started_cluster):
    db_name = "test_wide_insert_part"
    tb_name = "test_wide_insert_part"
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
    table_uuid = node.query(f"SELECT uuid FROM system.tables where database = '{db_name}' and table = '{tb_name}'").strip()
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid}'").strip() == "1"

def test_basic_merged_part(started_cluster):
    db_name = "test_basic_merged_part"
    tb_name = "test_basic_merged_part"
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
        SETTINGS old_parts_lifetime = 10
    """) )
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,2)")
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,3)")
    table_uuid = node.query(f"SELECT uuid FROM system.tables where database = '{db_name}' and table = '{tb_name}'").strip()
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid}'").strip() == "2"
    
    node.query(f"optimize table {db_name}.{tb_name}")
    
    time.sleep(30)
    
    assert node.query(f"SELECT count() FROM system.parts where database = '{db_name}' and table = '{tb_name}'").strip() == "1"
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid}'").strip() == "1"
    
def test_move_partition(started_cluster):
    db_name = "test_move_partition"
    tb_name_src = "test_move_partition_src"
    tb_name_dst = "test_move_partition_dst"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name_src}
        (
            `a` UInt64,
            `b` UInt64
        )
        ENGINE = MergeTree
        PARTITION BY a
        ORDER BY b
        SETTINGS old_parts_lifetime = 10
    """) )
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name_dst}
        (
            `a` UInt64,
            `b` UInt64
        )
        ENGINE = MergeTree
        PARTITION BY a
        ORDER BY b
        SETTINGS old_parts_lifetime = 10
    """) )
    node.query(f"INSERT INTO {db_name}.{tb_name_src} VALUES (1,2)")
    node.query(f"INSERT INTO {db_name}.{tb_name_src} VALUES (1,3)")
    node.query(f"INSERT INTO {db_name}.{tb_name_src} VALUES (1,4)")
    table_uuid_src = node.query(f"SELECT uuid FROM system.tables where database = '{db_name}' and table = '{tb_name_src}'").strip()
    table_uuid_dst = node.query(f"SELECT uuid FROM system.tables where database = '{db_name}' and table = '{tb_name_dst}'").strip()
    assert node.query(f"SELECT count() FROM system.parts where database = '{db_name}' and table = '{tb_name_src}'").strip() == "3"
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid_src}'").strip() == "3"
    
    node.query(f"ALTER TABLE {db_name}.{tb_name_src} MOVE PARTITION '1' TO TABLE {db_name}.{tb_name_dst} ")
    time.sleep(30)
    assert node.query(f"SELECT count() FROM system.parts where database = '{db_name}' and table = '{tb_name_src}'").strip() == "0"
    assert node.query(f"SELECT count() FROM system.parts where database = '{db_name}' and table = '{tb_name_dst}'").strip() == "3"
    
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid_src}'").strip() == "0"
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid_dst}'").strip() == "3"

def test_replace_partition(started_cluster):
    db_name = "test_replace_partition"
    tb_name_src = "test_replace_partition_src"
    tb_name_dst = "test_replace_partition_dst"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name_src}
        (
            `a` UInt64,
            `b` UInt64
        )
        ENGINE = MergeTree
        PARTITION BY a
        ORDER BY b
        SETTINGS old_parts_lifetime = 10
    """) )
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name_dst}
        (
            `a` UInt64,
            `b` UInt64
        )
        ENGINE = MergeTree
        PARTITION BY a
        ORDER BY b
        SETTINGS old_parts_lifetime = 10
    """) )
    node.query(f"INSERT INTO {db_name}.{tb_name_src} VALUES (1,2)")
    node.query(f"INSERT INTO {db_name}.{tb_name_src} VALUES (1,3)")
    node.query(f"INSERT INTO {db_name}.{tb_name_src} VALUES (1,4)")
    node.query(f"INSERT INTO {db_name}.{tb_name_dst} VALUES (1,2)")
    table_uuid_src = node.query(f"SELECT uuid FROM system.tables where database = '{db_name}' and table = '{tb_name_src}'").strip()
    table_uuid_dst = node.query(f"SELECT uuid FROM system.tables where database = '{db_name}' and table = '{tb_name_dst}'").strip()
    assert node.query(f"SELECT count() FROM system.parts where database = '{db_name}' and table = '{tb_name_src}'").strip() == "3"
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid_src}'").strip() == "3"
    assert node.query(f"SELECT count() FROM system.parts where database = '{db_name}' and table = '{tb_name_dst}'").strip() == "1"
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid_dst}'").strip() == "1"
    
    node.query(f"ALTER TABLE {db_name}.{tb_name_dst} REPLACE PARTITION '1' FROM {db_name}.{tb_name_src}")
    time.sleep(30)
    assert node.query(f"SELECT count() FROM system.parts where database = '{db_name}' and table = '{tb_name_src}'").strip() == "3"
    assert node.query(f"SELECT count() FROM system.parts where database = '{db_name}' and table = '{tb_name_dst}'").strip() == "3"
    
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid_src}'").strip() == "3"
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid_dst}'").strip() == "3"

def test_basic_update_part(started_cluster):
    db_name = "test_basic_update_part"
    tb_name = "test_basic_update_part"
    node = started_cluster.instances["node"]
    node.query(f"CREATE DATABASE {db_name}")
    node.query(dedent(f"""\
        CREATE TABLE {db_name}.{tb_name}
        (
            `a` UInt64,
            `b` UInt64,
            `c` UInt64
        )
        ENGINE = MergeTree
        PARTITION BY a
        ORDER BY b
        SETTINGS old_parts_lifetime = 10
    """) )
    node.query(f"INSERT INTO {db_name}.{tb_name} VALUES (1,2,3)")
    table_uuid = node.query(f"SELECT uuid FROM system.tables where database = '{db_name}' and table = '{tb_name}'").strip()
    assert node.query(f"SELECT count() FROM system.parts where database = '{db_name}' and table = '{tb_name}'").strip() == "1"
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid}'").strip() == "1"
    
    node.query(f"ALTER TABLE {db_name}.{tb_name} UPDATE c = c + 1 WHERE a = 1")
    time.sleep(30)
    assert node.query(f"SELECT count() FROM system.parts where database = '{db_name}' and table = '{tb_name}'").strip() == "1"
    assert node.query(f"SELECT count() FROM system.foundationdb where type = 'part' and key[1] = '{table_uuid}'").strip() == "1"