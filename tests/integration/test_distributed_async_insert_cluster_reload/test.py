import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"])
node3 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"])

# Same cluster as configs/remote_servers.xml, but shard 2 points to node3
# instead of node2. The directory queue of shard 2 on node1 stays the same
# (same `shard2_replica1` directory), only its destination changes.
config_with_node3 = """<clickhouse>
    <remote_servers>
        <test_cluster>
            <shard>
                <replica>
                    <host>node1</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <replica>
                    <host>node3</host>
                    <port>9000</port>
                </replica>
            </shard>
        </test_cluster>
    </remote_servers>
</clickhouse>"""

PAUSE_FAILPOINT = "distributed_async_insert_pause_before_mark_as_sent"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        for node in (node1, node2, node3):
            node.query(
                "CREATE TABLE local (key Int64, batch String) ENGINE = MergeTree ORDER BY key"
            )
        node1.query(
            "CREATE TABLE dist (key Int64, batch String) "
            "ENGINE = Distributed(test_cluster, currentDatabase(), local, key)"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_queue_survives_cluster_reload(started_cluster):
    """A remote_servers reload while a queued file is in flight must not recreate
    the directory queue: the replaced queue is still alive (mid-send) and removes
    its file on resume, which leaves a recreated queue retrying FILE_DOESNT_EXIST
    forever and can deliver the same file twice. The queue's connection pool is
    updated in place instead; files queued before the reload go to the new
    destination, nothing is lost, duplicated, or stuck.
    """
    # All inserted keys are odd, so every row routes to shard 2 (remote, async).
    node1.query(f"SYSTEM ENABLE FAILPOINT {PAUSE_FAILPOINT}")
    try:
        # Batch A is sent to node2, then the queue pauses before removing the file:
        # an in-flight send, frozen at the most race-prone point.
        node1.query("INSERT INTO dist SELECT number * 2 + 1, 'A' FROM numbers(5)")
        assert_eq_with_retry(node2, "SELECT count() FROM local WHERE batch = 'A'", "5")
        assert (
            int(
                node1.query(
                    "SELECT sum(data_files) FROM system.distribution_queue WHERE table = 'dist'"
                )
            )
            >= 1
        )

        # Batch B queues behind the paused send.
        node1.query("INSERT INTO dist SELECT number * 2 + 1, 'B' FROM numbers(5)")

        # Repoint shard 2 at node3 and reload.
        node1.replace_config(
            "/etc/clickhouse-server/config.d/remote_servers.xml", config_with_node3
        )
        node1.query("SYSTEM RELOAD CONFIG")
        assert_eq_with_retry(
            node1,
            "SELECT host_name FROM system.clusters WHERE cluster = 'test_cluster' AND shard_num = 2",
            "node3",
        )

        # Batch C's INSERT looks the queue up under the new cluster version while
        # the old send is still in flight. Without the in-place pool update this
        # recreates the queue and reproduces the race. The INSERT runs in the
        # background: on broken code, replacing the queue destroys the old one,
        # whose destructor waits for the paused send, so a synchronous INSERT
        # would block until the failpoint is released.
        insert_c = node1.get_query_request(
            "INSERT INTO dist SELECT number * 2 + 1, 'C' FROM numbers(5)"
        )
        time.sleep(2)
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {PAUSE_FAILPOINT}")
    insert_c.get_answer()

    # The queue drains without errors: batch A was already delivered to node2;
    # batches B and C go to the new destination.
    assert_eq_with_retry(
        node1,
        "SELECT sum(data_files), sum(error_count) FROM system.distribution_queue WHERE table = 'dist'",
        "0\t0",
        retry_count=60,
    )
    assert_eq_with_retry(node3, "SELECT count() FROM local", "10")

    # No row was lost, duplicated, or sent to the old destination after the reload.
    assert node2.query("SELECT count(), countDistinct(key) FROM local") == "5\t5\n"
    assert node2.query("SELECT countIf(batch != 'A') FROM local") == "0\n"
    assert (
        node3.query("SELECT countDistinct(key) FROM local WHERE batch = 'B'") == "5\n"
    )
    assert (
        node3.query("SELECT countDistinct(key) FROM local WHERE batch = 'C'") == "5\n"
    )
