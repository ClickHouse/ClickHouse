import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", user_configs=["configs/insert_limits.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_projection_rebuild_uses_only_required_columns(started_cluster):
    # Here we check that projection rebuild does not create too many temporary parts.
    # The size of temporary projection part is limited by min_insert_block_size_bytes/min_insert_block_size_rows, so they are changed in the config.

    node1.query("drop table if exists tab")
    node1.query("create table tab (x UInt64, y UInt64, data String codec(NONE), v UInt8, projection p (select _part_offset order by y)) engine = ReplacingMergeTree(v) order by x settings allow_part_offset_column_in_projections=1, deduplicate_merge_projection_mode='rebuild';")
    # Here we expect 3 parts to be inserted, contrilled by max_block_size=min_insert_block_size_rows=10000
    node1.query("insert into tab select number, number, rightPad('', 100, 'a'), 0 from numbers(30000) settings max_block_size=10000;")
    # Here we merge parts, and projections should be rebuild
    # Initially we kept `data` column in projection squash, ~10 temporary parts were created by min_insert_block_size_bytes limit
    node1.query("optimize table tab final settings mutations_sync=2, alter_sync=2;")
    node1.query("system flush logs;")

    uuid = node1.query("select uuid from system.tables where table = 'tab';").strip()
    cnt = node1.query("select count() from system.text_log where query_id like '{}::all_%_2' and message like '%Reading%from part p_%from the beginning of the part%'".format(uuid))
    # One projection part per source part
    assert (cnt == '3\n')
    # Here we check that _parent_part_offset is calculated properly. It was fixed in https://github.com/ClickHouse/ClickHouse/pull/93827
    assert(node1.query("select min(_parent_part_offset), max(_parent_part_offset) from mergeTreeProjection(default, tab, 'p')") == '0\t29999\n')
