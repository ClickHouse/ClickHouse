import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
initiator = cluster.add_instance("initiator")
worker = cluster.add_instance("worker")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_worker_checks_set_row_policy(started_cluster):
    for node in (initiator, worker):
        node.query("CREATE TABLE set_rp (k UInt64) ENGINE = Set")
        node.query("INSERT INTO set_rp VALUES (1), (2)")
        node.query("CREATE TABLE data_rp (k UInt64) ENGINE = MergeTree ORDER BY k")
        node.query("INSERT INTO data_rp VALUES (1), (2)")

    serialized_query = """
        SELECT count()
        FROM remote('worker', currentDatabase(), data_rp)
        WHERE k IN set_rp
        SETTINGS serialize_query_plan = 1,
                 enable_parallel_replicas = 0,
                 automatic_parallel_replicas_mode = 0
        """

    assert initiator.query(serialized_query) == "2\n"

    worker.query(
        "CREATE ROW POLICY set_rp_filter ON set_rp USING k = 1 TO default"
    )

    error = initiator.query_and_get_error(serialized_query)

    assert "ACCESS_DENIED" in error
    assert "Cannot use table default.set_rp" in error
    assert "because a row policy applies to it" in error
