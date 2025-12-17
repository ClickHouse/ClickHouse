import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag="25.10",
    with_installed_binary=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_strings_update_add_column(start_cluster):
    node1.query("create table tab (s String) engine = MergeTree order by tuple() settings min_bytes_for_wide_part=1")
    node1.query("insert into tab select 'abc' || toString(number) from numbers(100)")
    assert('100\n' == node1.query("select count() from tab where not ignore(*)"))

    node1.restart_with_latest_version()

    node1.query("alter table tab add column s3 String default 'def' settings alter_sync=2")
    node1.query("alter table tab MATERIALIZE column s3 settings alter_sync=2, mutations_sync=2, allow_experimental_statistics=1")
    node1.query("detach table tab")
    node1.query("attach table tab")
    assert('abc99\tdef\n' == node1.query("select max(s), max(s3) from tab"))
    # assert('1' == node1.query('check table tab'))


def test_strings_and_update(start_cluster):
    node1.query("create table tab (s String, x UInt32) engine = MergeTree order by tuple() settings min_bytes_for_wide_part=1")
    node1.query("insert into tab select 'abc', 1")
    assert('abc\n' == node1.query("select s from tab"))

    node1.restart_with_latest_version()

    node1.query("alter table tab add statistics x type Uniq settings alter_sync=2, allow_experimental_statistics=1")
    node1.query("alter table tab MATERIALIZE STATISTICS ALL settings alter_sync=2, allow_experimental_statistics=1")
    node1.query("detach table tab")
    node1.query("attach table tab")
    assert('abc\n' == node1.query("select s from tab"))
