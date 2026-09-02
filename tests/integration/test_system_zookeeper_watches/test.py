import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_zookeeper_watches_after_reconnect(start_cluster):
    instance.query(
        "CREATE TABLE test_watches (key UInt64, value String) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_watches', '1') ORDER BY key"
    )
    instance.query("INSERT INTO test_watches VALUES (1, 'a')")
    instance.query("SYSTEM SYNC REPLICA test_watches")

    initial_count = int(
        instance.query(
            "SELECT count() FROM system.zookeeper_watches WHERE path LIKE '%test_watches%'"
        ).strip()
    )
    assert initial_count > 0

    old_session = instance.query(
        "SELECT session_id FROM system.zookeeper_watches WHERE path LIKE '%test_watches%' LIMIT 1"
    ).strip()

    instance.query("SYSTEM RECONNECT ZOOKEEPER")

    # Wait for watches to be re-established with new session
    new_count = 0
    for _ in range(30):
        new_count = int(
            instance.query(
                "SELECT count() FROM system.zookeeper_watches WHERE path LIKE '%test_watches%'"
            ).strip()
        )
        if new_count > 0:
            break
        time.sleep(1)

    assert new_count > 0

    new_session = instance.query(
        "SELECT session_id FROM system.zookeeper_watches WHERE path LIKE '%test_watches%' LIMIT 1"
    ).strip()
    assert old_session != new_session

    instance.query("DROP TABLE test_watches SYNC")
