import pytest
import time

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage_config.xml"],
    tmpfs=["/with_merges:size=200M", "/no_merges:size=200M"],
)

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def wait_parts_count(table, expected_number_of_parts):
    for i in range(100):
        print(f"Waiting {expected_number_of_parts} for table {table}. Iteration: {i}")
        parts_count = int(node.query(f"select count() from system.parts where table = '{table}' and active"))

        if parts_count == expected_number_of_parts:
            break

        time.sleep(1)

    assert int(node.query(f"select count() from system.parts where table = '{table}' and active")) == expected_number_of_parts


def test_no_merges_volume_ttl_regular_merge(start_cluster):
    node.query("create table test_no_merges_volume_ttl (time DateTime) engine = MergeTree order by tuple() ttl time settings storage_policy='hot_cold_separation_policy', merge_with_ttl_timeout=0")
    table_uuid = node.query("select uuid from system.tables where table = 'test_no_merges_volume_ttl'").strip()

    node.query("system stop merges test_no_merges_volume_ttl")
    node.query("insert into test_no_merges_volume_ttl values (now() - interval 1 day)")
    assert node.query("select path from system.parts where table = 'test_no_merges_volume_ttl' and active").strip() == f"/with_merges/store/{table_uuid[:3]}/{table_uuid}/all_1_1_0/"
    assert int(node.query("select count() from test_no_merges_volume_ttl").strip()) == 1

    node.query("ALTER TABLE test_no_merges_volume_ttl MOVE PARTITION () TO VOLUME 'no_merges'")
    assert node.query("select path from system.parts where table = 'test_no_merges_volume_ttl' and active").strip() == f"/no_merges/store/{table_uuid[:3]}/{table_uuid}/all_1_1_0/"
    assert int(node.query("select count() from test_no_merges_volume_ttl").strip()) == 1

    node.query("system start merges test_no_merges_volume_ttl")
    wait_parts_count("test_no_merges_volume_ttl", 0)

    assert int(node.query("select count() from test_no_merges_volume_ttl").strip()) == 0
    node.query("drop table test_no_merges_volume_ttl sync")


def test_no_merges_volume_ttl_optimize(start_cluster):
    node.query("create table test_no_merges_volume_ttl_optimize (time DateTime) engine = MergeTree order by tuple() ttl time settings storage_policy='hot_cold_separation_policy', merge_with_ttl_timeout=0, do_not_run_schedule_data_processing_job=1")
    table_uuid = node.query("select uuid from system.tables where table = 'test_no_merges_volume_ttl_optimize'").strip()

    node.query("system stop merges test_no_merges_volume_ttl_optimize")
    node.query("insert into test_no_merges_volume_ttl_optimize values (now() - interval 1 day)")
    assert node.query("select path from system.parts where table = 'test_no_merges_volume_ttl_optimize' and active").strip() == f"/with_merges/store/{table_uuid[:3]}/{table_uuid}/all_1_1_0/"
    assert int(node.query("select count() from test_no_merges_volume_ttl_optimize").strip()) == 1

    node.query("ALTER TABLE test_no_merges_volume_ttl_optimize MOVE PARTITION () TO VOLUME 'no_merges'")
    assert node.query("select path from system.parts where table = 'test_no_merges_volume_ttl_optimize' and active").strip() == f"/no_merges/store/{table_uuid[:3]}/{table_uuid}/all_1_1_0/"
    assert int(node.query("select count() from test_no_merges_volume_ttl_optimize").strip()) == 1

    node.query("system start merges test_no_merges_volume_ttl_optimize")
    node.query("optimize table test_no_merges_volume_ttl_optimize")
    assert int(node.query("select count() from test_no_merges_volume_ttl_optimize").strip()) == 0
    node.query("drop table test_no_merges_volume_ttl_optimize sync")


def test_no_merges_volume_no_regular_merges(start_cluster):
    node.query("create table test_no_merges_volume_ttl_optimize (a UInt64) engine = MergeTree order by tuple() settings storage_policy='hot_cold_separation_policy'")
    table_uuid = node.query("select uuid from system.tables where table = 'test_no_merges_volume_ttl_optimize'").strip()

    node.query("system stop merges test_no_merges_volume_ttl_optimize")
    node.query("insert into test_no_merges_volume_ttl_optimize select number from numbers(50) settings max_block_size=1, min_insert_block_size_bytes=1")
    assert int(node.query("select count() from test_no_merges_volume_ttl_optimize").strip()) == 50
    assert int(node.query("select count() from system.parts where table = 'test_no_merges_volume_ttl_optimize' and active").strip()) == 50

    node.query("ALTER TABLE test_no_merges_volume_ttl_optimize MOVE PARTITION () TO VOLUME 'no_merges'")
    assert int(node.query("select count() from test_no_merges_volume_ttl_optimize").strip()) == 50
    assert int(node.query("select count() from system.parts where table = 'test_no_merges_volume_ttl_optimize' and active").strip()) == 50

    node.query("system start merges test_no_merges_volume_ttl_optimize")
    node.query("optimize table test_no_merges_volume_ttl_optimize")
    node.query("optimize table test_no_merges_volume_ttl_optimize")
    node.query("optimize table test_no_merges_volume_ttl_optimize")
    node.query("optimize table test_no_merges_volume_ttl_optimize")
    node.query("optimize table test_no_merges_volume_ttl_optimize final")
    node.query("optimize table test_no_merges_volume_ttl_optimize final")
    node.query("optimize table test_no_merges_volume_ttl_optimize final")
    node.query("optimize table test_no_merges_volume_ttl_optimize final")

    assert int(node.query("select count() from test_no_merges_volume_ttl_optimize").strip()) == 50
    assert int(node.query("select count() from system.parts where table = 'test_no_merges_volume_ttl_optimize' and active").strip()) == 50
