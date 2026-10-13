import threading
import uuid

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/enable_keeper.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)


def get_fake_zk():
    return keeper_utils.get_fake_zk(cluster, "node")


def stop_zk(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


def get_event_value(event_name):
    result = node.query(
        f"SELECT value FROM system.events WHERE event = '{event_name}'"
    ).strip()
    return int(result) if result else 0


def make_watch():
    """Return (event, callback). The callback is fired by kazoo once the watch is triggered."""
    fired = threading.Event()

    def callback(_watched_event):
        fired.set()

    return fired, callback


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_keeper_watch_triggered_profile_events(started_cluster):
    zk = None
    try:
        zk = get_fake_zk()

        root = f"/test_watch_events_{uuid.uuid4().hex[:8]}"
        zk.create(root)

        total_before = get_event_value("KeeperWatchesTriggered")
        created_before = get_event_value("KeeperWatchTriggeredNodeCreated")
        deleted_before = get_event_value("KeeperWatchTriggeredNodeDeleted")
        changed_before = get_event_value("KeeperWatchTriggeredNodeDataChanged")
        children_before = get_event_value("KeeperWatchTriggeredNodeChildrenChanged")

        created_path = f"{root}/created"
        fired, callback = make_watch()
        assert zk.exists(created_path, watch=callback) is None
        zk.create(created_path)
        assert fired.wait(timeout=30), "CREATED watch was not triggered"

        fired, callback = make_watch()
        zk.get(created_path, watch=callback)
        zk.set(created_path, b"new-data")
        assert fired.wait(timeout=30), "CHANGED watch was not triggered"

        fired, callback = make_watch()
        zk.get_children(root, watch=callback)
        child_path = f"{root}/child"
        zk.create(child_path)
        assert fired.wait(timeout=30), "CHILD watch was not triggered"

        fired, callback = make_watch()
        zk.get(child_path, watch=callback)
        zk.delete(child_path)
        assert fired.wait(timeout=30), "DELETED watch was not triggered"

        created_diff = get_event_value("KeeperWatchTriggeredNodeCreated") - created_before
        changed_diff = get_event_value("KeeperWatchTriggeredNodeDataChanged") - changed_before
        children_diff = (
            get_event_value("KeeperWatchTriggeredNodeChildrenChanged") - children_before
        )
        deleted_diff = get_event_value("KeeperWatchTriggeredNodeDeleted") - deleted_before
        total_diff = get_event_value("KeeperWatchesTriggered") - total_before

        assert created_diff >= 1, f"KeeperWatchTriggeredNodeCreated diff {created_diff} < 1"
        assert changed_diff >= 1, f"KeeperWatchTriggeredNodeDataChanged diff {changed_diff} < 1"
        assert (
            children_diff >= 1
        ), f"KeeperWatchTriggeredNodeChildrenChanged diff {children_diff} < 1"
        assert deleted_diff >= 1, f"KeeperWatchTriggeredNodeDeleted diff {deleted_diff} < 1"

        assert total_diff >= 4, f"KeeperWatchesTriggered diff {total_diff} < 4"
    finally:
        stop_zk(zk)
