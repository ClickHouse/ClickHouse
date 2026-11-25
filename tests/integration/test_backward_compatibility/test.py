import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/old_definer_shard.xml"],
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/wide_parts_only.xml",
        "configs/no_compress_marks.xml",
        "configs/default_serialization_info.xml",
        "configs/old_definer_shard.xml",
    ],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        create_query = """CREATE TABLE t(date Date, id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/t', '{}')
            PARTITION BY toYYYYMM(date)
            ORDER BY id"""
        node1.query(create_query.format(1))
        node1.query("DETACH TABLE t")  # stop being leader
        node2.query(create_query.format(2))
        node1.query("ATTACH TABLE t")
        yield cluster

    finally:
        cluster.shutdown()


def test_backward_compatability1(start_cluster):
    node2.query("INSERT INTO t VALUES (today(), 1)")
    node1.query("SYSTEM SYNC REPLICA t", timeout=10)

    assert node1.query("SELECT count() FROM t") == "1\n"


def test_new_client_and_initiator_with_negative_seconds_in_old_server_profile(start_cluster):
    node1.query("DROP USER IF EXISTS u_negative_seconds")
    node1.query("CREATE USER u_negative_seconds IDENTIFIED WITH no_password SETTINGS max_execution_time = -1")
    node1.query("GRANT SELECT ON system.one TO u_negative_seconds")
    try:
        # `node1.query*` runs the tested client against the old server; the client warns about the value it skips.
        out, err = node1.query_and_get_answer_with_error("SELECT 1", user="u_negative_seconds")
        assert out == "1\n"
        assert "Cannot parse the value of setting `max_execution_time`, skipping it" in err
        assert node2.query("SELECT count() FROM remote('node1', system.one, 'u_negative_seconds', '')") == "1\n"
    finally:
        node1.query("DROP USER IF EXISTS u_negative_seconds")


def test_definer_view_requires_a_capable_shard(start_cluster):
    node2.query("CREATE USER view_reader")
    try:
        node2.query("CREATE TABLE dist_one AS system.one ENGINE = Distributed(old_definer_shard, system, one)")
        node2.query("CREATE VIEW v DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT dummy FROM dist_one")
        node2.query("GRANT SELECT ON default.v TO view_reader")
        assert node2.query("SELECT dummy FROM dist_one") == "0\n"
        error = node2.query_and_get_error("SELECT dummy FROM v", user="view_reader")
        assert "forwarding the definer requires a cluster secret and protocol revision" in error
    finally:
        node2.query("DROP VIEW IF EXISTS v")
        node2.query("DROP TABLE IF EXISTS dist_one")
        node2.query("DROP USER view_reader")
