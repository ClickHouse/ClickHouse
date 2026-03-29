import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster

logging.basicConfig(level=logging.INFO)

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.d/storage_conf.xml"],
    with_minio=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.d/storage_conf.xml"],
    with_minio=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


TABLE_SETTINGS = (
    "storage_policy = 's3', "
    "leader_election = true, "
    "leader_election_heartbeat_interval = 1, "
    "leader_election_session_timeout = 5"
)

# Fixed UUID so both nodes share the same S3 data path and lease file.
SHARED_UUID = "12345678-abcd-abcd-abcd-123456789abc"
SHARED_UUID_FO = "12345678-abcd-abcd-abcd-123456789abd"


def create_table_on_first_node(node, table_name="test_le", uuid=SHARED_UUID):
    """Create the table on the first node (initializes the S3 directory)."""
    node.query(
        f"""
        CREATE TABLE {table_name} UUID '{uuid}' (x UInt64)
        ENGINE = MergeTree ORDER BY x
        SETTINGS {TABLE_SETTINGS}
        """
    )


def attach_table_on_second_node(node, table_name="test_le", uuid=SHARED_UUID):
    """Attach the table on the second node using the same UUID (shares S3 path)."""
    node.query(
        f"""
        ATTACH TABLE {table_name} UUID '{uuid}' (x UInt64)
        ENGINE = MergeTree ORDER BY x
        SETTINGS {TABLE_SETTINGS}
        """
    )


def is_leader(node, table_name="test_le"):
    """Check if the node considers itself the leader by attempting an insert."""
    try:
        node.query(f"INSERT INTO {table_name} VALUES (0)")
        # Clean up the test row
        node.query(
            f"ALTER TABLE {table_name} DELETE WHERE x = 0 SETTINGS mutations_sync = 1"
        )
        return True
    except Exception as e:
        if "TABLE_IS_READ_ONLY" in str(e):
            return False
        raise


def wait_for_leader(nodes, timeout=60, table_name="test_le"):
    """Wait until exactly one node becomes the leader. Returns (leader, followers)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        leaders = []
        followers = []
        for node in nodes:
            try:
                if is_leader(node, table_name):
                    leaders.append(node)
                else:
                    followers.append(node)
            except Exception:
                followers.append(node)
        if len(leaders) == 1 and len(followers) == len(nodes) - 1:
            return leaders[0], followers
        time.sleep(2)
    raise RuntimeError("Timed out waiting for exactly one leader")


def test_leader_elected(started_cluster):
    """Test that when two nodes share S3 storage, exactly one becomes leader."""
    create_table_on_first_node(node1)
    attach_table_on_second_node(node2)

    leader, followers = wait_for_leader([node1, node2])
    follower = followers[0]

    logging.info(f"Leader: {leader.name}, Follower: {follower.name}")

    # Leader can insert
    leader.query("INSERT INTO test_le VALUES (1), (2), (3)")
    assert leader.query("SELECT count() FROM test_le WHERE x > 0").strip() == "3"

    # Follower cannot insert
    error = ""
    try:
        follower.query("INSERT INTO test_le VALUES (100)")
    except Exception as e:
        error = str(e)
    assert "TABLE_IS_READ_ONLY" in error, f"Expected TABLE_IS_READ_ONLY, got: {error}"

    node1.query("DROP TABLE IF EXISTS test_le SYNC")
    node2.query("DROP TABLE IF EXISTS test_le SYNC")


def test_failover(started_cluster):
    """Test that when the leader stops, the follower takes over."""
    create_table_on_first_node(node1, "test_fo", SHARED_UUID_FO)
    attach_table_on_second_node(node2, "test_fo", SHARED_UUID_FO)

    leader, followers = wait_for_leader([node1, node2], table_name="test_fo")
    follower = followers[0]

    logging.info(f"Leader: {leader.name}, Follower: {follower.name}")

    # Leader inserts data
    leader.query("INSERT INTO test_fo VALUES (1), (2), (3)")

    # Stop the leader
    leader.stop_clickhouse()

    # Wait for the follower to become leader (session_timeout = 5s)
    deadline = time.monotonic() + 60
    new_leader = False
    while time.monotonic() < deadline:
        try:
            follower.query("INSERT INTO test_fo VALUES (10)")
            new_leader = True
            break
        except Exception as e:
            if "TABLE_IS_READ_ONLY" in str(e):
                time.sleep(2)
                continue
            raise

    assert new_leader, "Follower did not become leader after original leader stopped"
    logging.info(f"New leader: {follower.name}")

    # Restart the old leader
    leader.start_clickhouse()

    # The old leader should now be a follower (since the new leader holds the lease)
    time.sleep(5)  # Wait for the old leader to discover the lease

    # Verify data is accessible from the restarted node
    count = leader.query("SELECT count() FROM test_fo WHERE x > 0").strip()
    assert int(count) >= 3, f"Expected at least 3 rows, got {count}"

    node1.query("DROP TABLE IF EXISTS test_fo SYNC")
    node2.query("DROP TABLE IF EXISTS test_fo SYNC")
