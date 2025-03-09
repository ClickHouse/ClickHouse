#!/usr/bin/env python3
import os
import time

import pytest
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.retry import KazooRetry
from kazoo.security import make_acl

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper_config.xml", "configs/logs_conf.xml"],
    stay_alive=True,
)


def start_zookeeper():
    node.exec_in_container(["bash", "-c", "/opt/zookeeper/bin/zkServer.sh start"])


def stop_zookeeper():
    node.exec_in_container(["bash", "-c", "/opt/zookeeper/bin/zkServer.sh stop"])
    timeout = time.time() + 60
    while node.get_process_pid("zookeeper") != None:
        if time.time() > timeout:
            raise Exception("Failed to stop ZooKeeper in 60 secs")
        time.sleep(0.2)


def clear_zookeeper():
    node.exec_in_container(["bash", "-c", "rm -fr /zookeeper/*"])


def restart_and_clear_zookeeper():
    stop_zookeeper()
    clear_zookeeper()
    start_zookeeper()


def restart_zookeeper():
    stop_zookeeper()
    start_zookeeper()


def generate_zk_snapshot():
    for _ in range(100):
        stop_zookeeper()
        start_zookeeper()
        time.sleep(2)
        stop_zookeeper()

        # get last snapshot
        last_snapshot = node.exec_in_container(
            [
                "bash",
                "-c",
                "find /zookeeper/version-2 -name 'snapshot.*' -printf '%T@ %p\n' | sort -n | awk 'END {print $2}'",
            ]
        ).strip()

        print(f"Latest snapshot: {last_snapshot}")

        try:
            # verify last snapshot
            # zkSnapShotToolkit is a tool to inspect generated snapshots - if it's broken, an exception is thrown
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"/opt/zookeeper/bin/zkSnapShotToolkit.sh {last_snapshot}",
                ]
            )
            return
        except Exception as err:
            print(f"Got error while reading snapshot: {err}")

    raise Exception("Failed to generate a ZooKeeper snapshot")


def clear_clickhouse_data():
    node.exec_in_container(
        [
            "bash",
            "-c",
            "rm -fr /var/lib/clickhouse/coordination/logs/* /var/lib/clickhouse/coordination/snapshots/*",
        ]
    )


def convert_zookeeper_data():
    node.exec_in_container(
        [
            "bash",
            "-c",
            "tar -cvzf /var/lib/clickhouse/zk-data.tar.gz /zookeeper/version-2",
        ]
    )

    cmd = "/usr/bin/clickhouse keeper-converter --zookeeper-logs-dir /zookeeper/version-2/ --zookeeper-snapshots-dir  /zookeeper/version-2/ --output-dir /var/lib/clickhouse/coordination/snapshots"
    node.exec_in_container(["bash", "-c", cmd])


def stop_clickhouse():
    node.stop_clickhouse()


def start_clickhouse():
    node.start_clickhouse()
    keeper_utils.wait_until_connected(cluster, node)


def copy_zookeeper_data(make_zk_snapshots):
    if make_zk_snapshots:  # force zookeeper to create snapshot
        generate_zk_snapshot()
    else:
        stop_zookeeper()

    stop_clickhouse()
    clear_clickhouse_data()
    convert_zookeeper_data()
    start_zookeeper()
    start_clickhouse()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_fake_zk(timeout=60.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip("node") + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


def get_genuine_zk(timeout=60.0):
    CONNECTION_RETRIES = 100
    for i in range(CONNECTION_RETRIES):
        try:
            _genuine_zk_instance = KazooClient(
                hosts=cluster.get_instance_ip("node") + ":2181",
                timeout=timeout,
                connection_retry=KazooRetry(max_tries=20),
            )
            _genuine_zk_instance.start()
            return _genuine_zk_instance
        except KazooTimeoutError:
            if i == CONNECTION_RETRIES - 1:
                raise

            print(
                "Failed to connect to ZK cluster because of timeout. Restarting cluster and trying again."
            )
            time.sleep(0.2)
            restart_zookeeper()


def compare_stats(stat1, stat2, path, ignore_pzxid=False):
    assert stat1.czxid == stat2.czxid, (
        "path "
        + path
        + " cxzids not equal for stats: "
        + str(stat1.czxid)
        + " != "
        + str(stat2.zxid)
    )
    assert stat1.mzxid == stat2.mzxid, (
        "path "
        + path
        + " mxzids not equal for stats: "
        + str(stat1.mzxid)
        + " != "
        + str(stat2.mzxid)
    )
    assert stat1.version == stat2.version, (
        "path "
        + path
        + " versions not equal for stats: "
        + str(stat1.version)
        + " != "
        + str(stat2.version)
    )
    assert stat1.cversion == stat2.cversion, (
        "path "
        + path
        + " cversions not equal for stats: "
        + str(stat1.cversion)
        + " != "
        + str(stat2.cversion)
    )
    assert stat1.aversion == stat2.aversion, (
        "path "
        + path
        + " aversions not equal for stats: "
        + str(stat1.aversion)
        + " != "
        + str(stat2.aversion)
    )
    assert stat1.ephemeralOwner == stat2.ephemeralOwner, (
        "path "
        + path
        + " ephemeralOwners not equal for stats: "
        + str(stat1.ephemeralOwner)
        + " != "
        + str(stat2.ephemeralOwner)
    )
    assert stat1.dataLength == stat2.dataLength, (
        "path "
        + path
        + " ephemeralOwners not equal for stats: "
        + str(stat1.dataLength)
        + " != "
        + str(stat2.dataLength)
    )
    assert stat1.numChildren == stat2.numChildren, (
        "path "
        + path
        + " numChildren not equal for stats: "
        + str(stat1.numChildren)
        + " != "
        + str(stat2.numChildren)
    )
    if not ignore_pzxid:
        assert stat1.pzxid == stat2.pzxid, (
            "path "
            + path
            + " pzxid not equal for stats: "
            + str(stat1.pzxid)
            + " != "
            + str(stat2.pzxid)
        )


def compare_states(zk1, zk2, path="/", exclude_paths=[]):
    data1, stat1 = zk1.get(path)
    data2, stat2 = zk2.get(path)
    print("Left Stat", stat1)
    print("Right Stat", stat2)
    assert data1 == data2, "Data not equal on path " + str(path)
    # both paths have strange stats
    if path not in ("/", "/zookeeper") and path not in exclude_paths:
        compare_stats(stat1, stat2, path)

    first_children = list(sorted(zk1.get_children(path)))
    second_children = list(sorted(zk2.get_children(path)))
    print("Got children left", first_children)
    print("Got children rigth", second_children)

    if path == "/":
        assert set(first_children) ^ set(second_children) == set(["keeper"])
    else:
        assert first_children == second_children, (
            "Childrens are not equal on path " + path
        )

    for children in first_children:
        if path != "/" or children != "keeper":
            print("Checking child", os.path.join(path, children))
            compare_states(zk1, zk2, os.path.join(path, children), exclude_paths)


@pytest.mark.parametrize(("create_snapshots"), [True, False])
def test_smoke(started_cluster, create_snapshots):
    restart_and_clear_zookeeper()

    genuine_connection = get_genuine_zk()
    genuine_connection.create("/test", b"data")

    assert genuine_connection.get("/test")[0] == b"data"

    copy_zookeeper_data(create_snapshots)

    genuine_connection = get_genuine_zk()
    fake_connection = get_fake_zk()

    compare_states(genuine_connection, fake_connection)

    genuine_connection.stop()
    genuine_connection.close()

    fake_connection.stop()
    fake_connection.close()


def get_bytes(s):
    return s.encode()


def assert_ephemeral_disappear(connection, path):
    for _ in range(200):
        if not connection.exists(path):
            break

        time.sleep(0.1)
    else:
        raise Exception("ZK refuse to remove ephemeral nodes")


@pytest.mark.parametrize(("create_snapshots"), [True, False])
def test_simple_crud_requests(started_cluster, create_snapshots):
    restart_and_clear_zookeeper()

    genuine_connection = get_genuine_zk(timeout=5)
    for i in range(100):
        genuine_connection.create("/test_create" + str(i), get_bytes("data" + str(i)))

    # some set queries
    for i in range(10):
        for j in range(i + 1):
            genuine_connection.set("/test_create" + str(i), get_bytes("value" + str(j)))

    for i in range(10, 20):
        genuine_connection.delete("/test_create" + str(i))

    path = "/test_create_deep"
    for i in range(10):
        genuine_connection.create(path, get_bytes("data" + str(i)))
        path = os.path.join(path, str(i))

    genuine_connection.create("/test_sequential", b"")
    for i in range(10):
        genuine_connection.create(
            "/test_sequential/" + "a" * i + "-",
            get_bytes("dataX" + str(i)),
            sequence=True,
        )

    genuine_connection.create("/test_ephemeral", b"")
    for i in range(10):
        genuine_connection.create(
            "/test_ephemeral/" + str(i), get_bytes("dataX" + str(i)), ephemeral=True
        )

    copy_zookeeper_data(create_snapshots)

    genuine_connection.stop()
    genuine_connection.close()

    genuine_connection = get_genuine_zk(timeout=5)

    fake_connection = get_fake_zk(timeout=5)
    for conn in [genuine_connection, fake_connection]:
        assert_ephemeral_disappear(conn, "/test_ephemeral/0")

    # After receiving close request zookeeper updates pzxid of ephemeral parent.
    # Keeper doesn't receive such request (snapshot created before it) so it doesn't do it.
    compare_states(
        genuine_connection, fake_connection, exclude_paths=["/test_ephemeral"]
    )
    eph1, stat1 = fake_connection.get("/test_ephemeral")
    eph2, stat2 = genuine_connection.get("/test_ephemeral")

    assert eph1 == eph2
    compare_stats(stat1, stat2, "/test_ephemeral", ignore_pzxid=True)

    # especially ensure that counters are the same
    genuine_connection.create(
        "/test_sequential/" + "a" * 10 + "-", get_bytes("dataX" + str(i)), sequence=True
    )
    fake_connection.create(
        "/test_sequential/" + "a" * 10 + "-", get_bytes("dataX" + str(i)), sequence=True
    )

    first_children = list(sorted(genuine_connection.get_children("/test_sequential")))
    second_children = list(sorted(fake_connection.get_children("/test_sequential")))
    assert first_children == second_children, "Childrens are not equal on path " + path

    genuine_connection.stop()
    genuine_connection.close()

    fake_connection.stop()
    fake_connection.close()


@pytest.mark.parametrize(("create_snapshots"), [True, False])
def test_multi_and_failed_requests(started_cluster, create_snapshots):
    restart_and_clear_zookeeper()

    genuine_connection = get_genuine_zk(timeout=5)
    genuine_connection.create("/test_multitransactions")
    for i in range(10):
        t = genuine_connection.transaction()
        t.create("/test_multitransactions/freddy" + str(i), get_bytes("data" + str(i)))
        t.create(
            "/test_multitransactions/fred" + str(i),
            get_bytes("value" + str(i)),
            ephemeral=True,
        )
        t.create(
            "/test_multitransactions/smith" + str(i),
            get_bytes("entity" + str(i)),
            sequence=True,
        )
        t.set_data("/test_multitransactions", get_bytes("somedata" + str(i)))
        t.commit()

    with pytest.raises(Exception):
        genuine_connection.set(
            "/test_multitransactions/freddy0", get_bytes("mustfail" + str(i)), version=1
        )

    t = genuine_connection.transaction()

    t.create("/test_bad_transaction", get_bytes("data" + str(1)))
    t.check("/test_multitransactions", version=32)
    t.create("/test_bad_transaction1", get_bytes("data" + str(2)))
    # should fail
    t.commit()

    assert genuine_connection.exists("/test_bad_transaction") is None
    assert genuine_connection.exists("/test_bad_transaction1") is None

    t = genuine_connection.transaction()
    t.create("/test_bad_transaction2", get_bytes("data" + str(1)))
    t.delete("/test_multitransactions/freddy0", version=5)

    # should fail
    t.commit()
    assert genuine_connection.exists("/test_bad_transaction2") is None
    assert genuine_connection.exists("/test_multitransactions/freddy0") is not None

    copy_zookeeper_data(create_snapshots)

    genuine_connection.stop()
    genuine_connection.close()

    genuine_connection = get_genuine_zk(timeout=5)
    fake_connection = get_fake_zk(timeout=5)

    for conn in [genuine_connection, fake_connection]:
        assert_ephemeral_disappear(conn, "/test_multitransactions/fred0")

    # After receiving close request zookeeper updates pzxid of ephemeral parent.
    # Keeper doesn't receive such request (snapshot created before it) so it doesn't do it.
    compare_states(
        genuine_connection, fake_connection, exclude_paths=["/test_multitransactions"]
    )
    eph1, stat1 = fake_connection.get("/test_multitransactions")
    eph2, stat2 = genuine_connection.get("/test_multitransactions")

    assert eph1 == eph2
    compare_stats(stat1, stat2, "/test_multitransactions", ignore_pzxid=True)

    genuine_connection.stop()
    genuine_connection.close()

    fake_connection.stop()
    fake_connection.close()


@pytest.mark.parametrize(("create_snapshots"), [True, False])
def test_acls(started_cluster, create_snapshots):
    restart_and_clear_zookeeper()
    genuine_connection = get_genuine_zk()
    genuine_connection.add_auth("digest", "user1:password1")
    genuine_connection.add_auth("digest", "user2:password2")
    genuine_connection.add_auth("digest", "user3:password3")

    genuine_connection.create(
        "/test_multi_all_acl", b"data", acl=[make_acl("auth", "", all=True)]
    )

    other_connection = get_genuine_zk()
    other_connection.add_auth("digest", "user1:password1")
    other_connection.set("/test_multi_all_acl", b"X")
    assert other_connection.get("/test_multi_all_acl")[0] == b"X"

    yet_other_auth_connection = get_genuine_zk()
    yet_other_auth_connection.add_auth("digest", "user2:password2")

    yet_other_auth_connection.set("/test_multi_all_acl", b"Y")

    genuine_connection.add_auth("digest", "user3:password3")

    # just to check that we are able to deserialize it
    genuine_connection.set_acls(
        "/test_multi_all_acl",
        acls=[
            make_acl(
                "auth", "", read=True, write=False, create=True, delete=True, admin=True
            )
        ],
    )

    no_auth_connection = get_genuine_zk()

    with pytest.raises(Exception):
        no_auth_connection.set("/test_multi_all_acl", b"Z")

    copy_zookeeper_data(create_snapshots)

    genuine_connection = get_genuine_zk()
    genuine_connection.add_auth("digest", "user1:password1")
    genuine_connection.add_auth("digest", "user2:password2")
    genuine_connection.add_auth("digest", "user3:password3")

    fake_connection = get_fake_zk()
    fake_connection.add_auth("digest", "user1:password1")
    fake_connection.add_auth("digest", "user2:password2")
    fake_connection.add_auth("digest", "user3:password3")

    compare_states(genuine_connection, fake_connection)

    for connection in [genuine_connection, fake_connection]:
        acls, stat = connection.get_acls("/test_multi_all_acl")
        assert stat.aversion == 1
        assert len(acls) == 3
        for acl in acls:
            assert acl.acl_list == ["READ", "CREATE", "DELETE", "ADMIN"]
            assert acl.id.scheme == "digest"
            assert acl.perms == 29
        assert acl.id.id in (
            "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=",
            "user2:lo/iTtNMP+gEZlpUNaCqLYO3i5U=",
            "user3:wr5Y0kEs9nFX3bKrTMKxrlcFeWo=",
        )

    genuine_connection.stop()
    genuine_connection.close()

    fake_connection.stop()
    fake_connection.close()
