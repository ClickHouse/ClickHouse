# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# The `all_...` union tables read both the rotated versions of the log tables
# and the tables across the cluster.
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/clusters.xml",
        "configs/union_merge_and_cluster.xml",
    ],
)

# Only the tables across the cluster, without the rotated versions.
node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/clusters.xml",
        "configs/union_cluster.xml",
    ],
    stay_alive=True,
)

# Only the rotated versions of the local log tables.
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/union_merge.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_merge_rotated_tables(start_cluster):
    node3.query("SELECT 'test_merge_marker_first'")
    node3.query("SYSTEM FLUSH LOGS query_log")

    create_query = node3.query("SHOW CREATE TABLE system.all_query_log FORMAT TSVRaw")
    assert "AS merge('system', '^query_log(_[0-9]+)?$')" in create_query
    assert "clusterAllReplicas" not in create_query

    # The union table has the same structure as the log table.
    assert node3.query("DESCRIBE TABLE system.all_query_log") == node3.query(
        "DESCRIBE TABLE system.query_log"
    )

    assert (
        int(
            node3.query(
                "SELECT count() > 0 FROM system.all_query_log WHERE query LIKE '%test_merge_marker_first%'"
            )
        )
        == 1
    )

    # Union tables are created for all system logs configured on the server.
    node3.query("SYSTEM FLUSH LOGS text_log, trace_log")
    assert "AS merge" in node3.query(
        "SHOW CREATE TABLE system.all_text_log FORMAT TSVRaw"
    )
    assert "AS merge" in node3.query(
        "SHOW CREATE TABLE system.all_trace_log FORMAT TSVRaw"
    )


def test_recreated_after_drop(start_cluster):
    node3.query("SYSTEM FLUSH LOGS query_log")
    node3.query("DROP TABLE system.all_query_log SYNC")

    node3.query("SELECT 'test_drop_marker'")
    node3.query("SYSTEM FLUSH LOGS query_log")

    assert "AS merge" in node3.query(
        "SHOW CREATE TABLE system.all_query_log FORMAT TSVRaw"
    )


def test_recreated_on_rotation(start_cluster):
    node3.query("SELECT 'test_rotation_marker_before'")
    node3.query("SYSTEM FLUSH LOGS query_log")

    # A user-modified union table is left intact until the next rotation of the log table
    # (or a server restart).
    node3.query("DROP TABLE system.all_query_log SYNC")
    node3.query("CREATE TABLE system.all_query_log (x UInt8) ENGINE = Memory")
    node3.query("SYSTEM FLUSH LOGS query_log")
    assert "Memory" in node3.query(
        "SHOW CREATE TABLE system.all_query_log FORMAT TSVRaw"
    )

    # Trigger a rotation: the structure of the log table no longer matches the expected
    # one, so at the first flush after a restart it is renamed to `query_log_N` and
    # created anew, and the union table is recreated as well.
    node3.query("ALTER TABLE system.query_log ADD COLUMN test_rotation UInt8")
    node3.restart_clickhouse()
    node3.query("SYSTEM FLUSH LOGS query_log")

    assert node3.query("EXISTS TABLE system.query_log_0").strip() == "1"
    assert "AS merge" in node3.query(
        "SHOW CREATE TABLE system.all_query_log FORMAT TSVRaw"
    )

    node3.query("SELECT 'test_rotation_marker_after'")
    node3.query("SYSTEM FLUSH LOGS query_log")

    # The union table sees both the rotated table and the new one.
    assert (
        int(
            node3.query(
                "SELECT count() > 0 FROM system.all_query_log WHERE query LIKE '%test_rotation_marker_before%'"
            )
        )
        == 1
    )
    assert (
        int(
            node3.query(
                "SELECT count() > 0 FROM system.all_query_log WHERE query LIKE '%test_rotation_marker_after%'"
            )
        )
        == 1
    )


def test_stable_across_restarts(start_cluster):
    node3.query("SYSTEM FLUSH LOGS query_log")
    uuid_before = node3.query(
        "SELECT uuid FROM system.tables WHERE database = 'system' AND name = 'all_query_log'"
    )

    node3.restart_clickhouse()

    # The generated table definition must match the stored one, otherwise the table
    # would be recreated at the first flush after every restart.
    node3.query("SYSTEM FLUSH LOGS query_log")
    uuid_after = node3.query(
        "SELECT uuid FROM system.tables WHERE database = 'system' AND name = 'all_query_log'"
    )
    assert uuid_before == uuid_after


def test_cluster(start_cluster):
    node1.query("SELECT 'test_cluster_marker_node1'")
    node1.query("SYSTEM FLUSH LOGS query_log")
    node2.query("SELECT 'test_cluster_marker_node2'")
    node2.query("SYSTEM FLUSH LOGS query_log")

    create_query_node1 = node1.query(
        "SHOW CREATE TABLE system.all_query_log FORMAT TSVRaw"
    )
    assert (
        "AS clusterAllReplicas('system_logs_cluster', merge('system', '^query_log(_[0-9]+)?$'), SETTINGS skip_unavailable_shards = true)"
        in create_query_node1
    )

    create_query_node2 = node2.query(
        "SHOW CREATE TABLE system.all_query_log FORMAT TSVRaw"
    )
    assert (
        "AS clusterAllReplicas('system_logs_cluster', 'system', 'query_log', SETTINGS skip_unavailable_shards = true)"
        in create_query_node2
    )

    # The union table reads from all replicas of the cluster.
    for marker in ["test_cluster_marker_node1", "test_cluster_marker_node2"]:
        assert (
            int(
                node1.query(
                    f"SELECT count() > 0 FROM system.all_query_log WHERE query LIKE '%{marker}%'"
                )
            )
            == 1
        )

    assert (
        node1.query(
            "SELECT countDistinct(hostName()) FROM system.all_query_log"
        ).strip()
        == "2"
    )


def test_skip_unavailable_replicas(start_cluster):
    node1.query("SELECT 'test_skip_unavailable_marker'")
    node1.query("SYSTEM FLUSH LOGS query_log")

    node2.stop_clickhouse()
    try:
        # Reading does not fail because of the unavailable replica.
        assert (
            int(
                node1.query(
                    "SELECT count() > 0 FROM system.all_query_log WHERE query LIKE '%test_skip_unavailable_marker%'"
                )
            )
            == 1
        )
        assert (
            node1.query(
                "SELECT countDistinct(hostName()) FROM system.all_query_log"
            ).strip()
            == "1"
        )

        # The setting from the table definition can be overridden in the query.
        assert "Exception" in node1.query_and_get_error(
            "SELECT count() FROM system.all_query_log SETTINGS skip_unavailable_shards = 0"
        )
    finally:
        node2.start_clickhouse()
