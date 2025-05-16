#!/usr/bin/env python3
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_compressed_marks_restart_compact():
    node.query(
        "create table test_02381_compact (a UInt64, b String) ENGINE = MergeTree order by (a, b)"
    )
    node.query("insert into test_02381_compact values (1, 'Hello')")
    node.query(
        "alter table test_02381_compact modify setting compress_marks=true, compress_primary_key=true"
    )
    node.query("insert into test_02381_compact values (2, 'World')")
    node.query("optimize table test_02381_compact final")

    assert (
        node.query("SELECT count() FROM test_02381_compact WHERE not ignore(*)")
        == "2\n"
    )
    node.restart_clickhouse()
    assert (
        node.query("SELECT count() FROM test_02381_compact WHERE not ignore(*)")
        == "2\n"
    )


def test_compressed_marks_restart_wide():
    node.query(
        "create table test_02381_wide (a UInt64, b String) ENGINE = MergeTree order by (a, b) SETTINGS min_bytes_for_wide_part=0"
    )
    node.query("insert into test_02381_wide values (1, 'Hello')")
    node.query(
        "alter table test_02381_wide modify setting compress_marks=true, compress_primary_key=true"
    )
    node.query("insert into test_02381_wide values (2, 'World')")
    node.query("optimize table test_02381_wide final")

    assert (
        node.query("SELECT count() FROM test_02381_wide WHERE not ignore(*)") == "2\n"
    )
    node.restart_clickhouse()
    assert (
        node.query("SELECT count() FROM test_02381_wide WHERE not ignore(*)") == "2\n"
    )
