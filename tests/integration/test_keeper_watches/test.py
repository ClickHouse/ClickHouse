import time

import pytest
from kazoo.exceptions import BadVersionError

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

# from kazoo.protocol.serialization import Connect, read_buffer, write_buffer

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper_config1.xml"],
    stay_alive=True,
    use_keeper=False,
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def destroy_zk_client(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


def wait_nodes():
    keeper_utils.wait_nodes(cluster, [node])


def get_fake_zk(nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def test_keeper_watches(started_cluster):
    wait_nodes()

    node_zk = get_fake_zk(node.name)

    if node_zk.exists("/watchednode"):
        node_zk.delete("/watchednode")

    node_zk.create("/watchednode")
    watch_triggers = 0

    def watch_callback(event):
        if event.type == "CHANGED":
            nonlocal watch_triggers
            watch_triggers += 1

    def set_watch():
        node_zk.get("/watchednode", watch=watch_callback)

    def assert_watch_triggers(expected_count):
        for _ in range(100):
            assert watch_triggers <= expected_count
            if watch_triggers == expected_count:
                break
            time.sleep(0.5)
        else:
            assert watch_triggers == expected_count

    set_watch()
    node_zk.set("/watchednode", b"new_data")
    assert_watch_triggers(1)

    set_watch()
    with pytest.raises(BadVersionError):
        node_zk.set("/watchednode", b"new_data", version=9999)
    time.sleep(1)
    assert_watch_triggers(1)

    set_watch()
    tx = node_zk.transaction()
    tx.set_data("/watchednode", b"new_data2")
    tx.commit()
    assert_watch_triggers(2)

    set_watch()
    tx = node_zk.transaction()
    tx.set_data("/watchednode", b"new_data2", version=9998)
    results = tx.commit()
    assert len(results) == 1 and isinstance(results[0], BadVersionError)
    time.sleep(1)
    assert_watch_triggers(2)

    destroy_zk_client(node_zk)
    node_zk = None
    time.sleep(1)


def test_ephemeral_watch_session_close(started_cluster):
    wait_nodes()

    zk = get_fake_zk(node.name)


    def watch_callback(event):
        pass
    try:
        zk.create("/ephemeral_test_node", b"data", ephemeral=True)
        zk.create("/ephemeral_test_node_the_second", b"data", ephemeral=True)
        zk.exists("/ephemeral_test_node", watch=watch_callback)
        zk.exists("/ephemeral_test_node_the_second", watch=watch_callback)
    finally:
        destroy_zk_client(zk)

    data = keeper_utils.send_4lw_cmd(started_cluster, node, cmd="mntr")
    assert "zk_watch_count\t0" in data