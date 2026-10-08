#!/usr/bin/env python3

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_session_id():
    return int(
        node.query(
            "SELECT client_id FROM system.zookeeper_connection WHERE name = 'default'"
        ).strip()
    )


def test_zookeeper_session_on_config_reload(started_cluster):
    # Make the session observe some non-zero zxid.
    node.query(
        "CREATE TABLE t (key UInt64) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/t', 'r1') ORDER BY key"
    )

    # Reloading an unchanged config must keep the session.
    session_id = get_session_id()
    node.query("SYSTEM RELOAD CONFIG")
    assert get_session_id() == session_id

    # Expire the session and touch ZooKeeper to establish a new one. The new
    # session remembers the last zxid seen by the expired one (to refuse
    # connecting to a Keeper node that is behind).
    node.query("SYSTEM RECONNECT ZOOKEEPER")
    node.query("SELECT * FROM system.zookeeper WHERE path = '/'")

    new_session_id = get_session_id()
    assert new_session_id != session_id
    assert (
        int(
            node.query(
                "SELECT last_zxid_seen FROM system.zookeeper_connection WHERE name = 'default'"
            ).strip()
        )
        > 0
    )

    # Reloading an unchanged config must keep the session even after the
    # replacement (the remembered zxid is session state, not configuration).
    node.query("SYSTEM RELOAD CONFIG")
    assert get_session_id() == new_session_id
