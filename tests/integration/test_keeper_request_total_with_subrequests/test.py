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


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_keeper_request_total_with_subrequests(started_cluster):
    zk = None
    try:
        zk = get_fake_zk()

        test_root = f"/test_subrequests_{uuid.uuid4().hex[:8]}"

        # Warm up: create a root node for the test so Keeper connection is fully established
        zk.create(test_root)

        # Capture baseline
        total_before = get_event_value("KeeperRequestTotal")
        total_sub_before = get_event_value("KeeperRequestTotalWithSubrequests")

        # 1) Single requests: 5 creates (each counts as 1 for both metrics)
        num_single = 5
        for i in range(num_single):
            zk.create(f"{test_root}/single_{i}", b"data")

        # 2) Multi-request (transaction) with 10 subrequests
        num_multi_ops = 10
        txn = zk.transaction()
        for i in range(num_multi_ops):
            txn.create(f"{test_root}/multi_{i}", b"data")
        txn.commit()

        # Capture after
        total_after = get_event_value("KeeperRequestTotal")
        total_sub_after = get_event_value("KeeperRequestTotalWithSubrequests")

        total_diff = total_after - total_before
        total_sub_diff = total_sub_after - total_sub_before

        # KeeperRequestTotal: 5 single + 1 multi = 6 requests minimum
        # (there may be background keeper requests, so use >=)
        assert total_diff >= num_single + 1, (
            f"KeeperRequestTotal diff {total_diff} < expected {num_single + 1}"
        )

        # KeeperRequestTotalWithSubrequests: 5 single + 10 subrequests = 15 minimum
        assert total_sub_diff >= num_single + num_multi_ops, (
            f"KeeperRequestTotalWithSubrequests diff {total_sub_diff} < expected {num_single + num_multi_ops}"
        )

        # The difference between the two metrics should be at least (num_multi_ops - 1),
        # because the multi-request contributes 1 to Total but num_multi_ops to WithSubrequests
        assert total_sub_diff - total_diff >= num_multi_ops - 1, (
            f"Difference between metrics ({total_sub_diff - total_diff}) "
            f"< expected ({num_multi_ops - 1})"
        )
    finally:
        stop_zk(zk)
