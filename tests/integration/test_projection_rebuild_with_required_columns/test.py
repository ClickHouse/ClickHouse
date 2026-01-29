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
    node1.query("drop table if exists tab")
    node1.query("create table tab (x UInt64, y UInt64, data String codec(NONE), v UInt8, projection p (select _part_offset order by y)) engine = ReplacingMergeTree(v) order by x settings allow_part_offset_column_in_projections=1, deduplicate_merge_projection_mode='rebuild';")
    node1.query("insert into tab select number, number, rightPad('', 100, 'a'), 0 from numbers(30000);")
    node1.query("optimize table tab final settings mutations_sync=2, alter_sync=2;")
    node1.query("system flush logs;")

    uuid = node1.query("select uuid from system.tables where table = 'tab';").strip()
    cnt = node1.query("select count() from system.text_log where query_id like '{}::all_%_2' and message like '%Reading%from part p_%from the beginning of the part%'".format(uuid))
    assert (cnt == '2\n')

