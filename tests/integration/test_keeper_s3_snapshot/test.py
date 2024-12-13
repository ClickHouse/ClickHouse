from multiprocessing.dummy import Pool
from time import sleep

import pytest
from kazoo.client import KazooClient
from minio.deleteobjects import DeleteObject

from helpers import keeper_utils
from helpers.cluster import ClickHouseCluster
from helpers.retry_decorator import retry

# from kazoo.protocol.serialization import Connect, read_buffer, write_buffer

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/keeper_config1.xml"],
    stay_alive=True,
    with_minio=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/keeper_config2.xml"],
    stay_alive=True,
    with_minio=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/keeper_config3.xml"],
    stay_alive=True,
    with_minio=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        cluster.minio_client.make_bucket("snapshots")

        yield cluster

    finally:
        cluster.shutdown()


def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


def destroy_zk_client(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


def wait_node(node):
    for _ in range(100):
        zk = None
        try:
            zk = get_fake_zk(node.name, timeout=30.0)
            zk.sync("/")
            print("node", node.name, "ready")
            break
        except Exception as ex:
            sleep(0.2)
            print("Waiting until", node.name, "will be ready, exception", ex)
        finally:
            destroy_zk_client(zk)
    else:
        raise Exception("Can't wait node", node.name, "to become ready")


def delete_keeper_snapshots_logs(nodex):
    nodex.exec_in_container(
        [
            "bash",
            "-c",
            "rm -rf /var/lib/clickhouse/coordination/log /var/lib/clickhouse/coordination/snapshots",
        ]
    )


def test_s3_upload(started_cluster):

    node1_zk = get_fake_zk(node1.name)

    # we defined in configs snapshot_distance as 50
    # so after 50 requests we should generate a snapshot
    for _ in range(210):
        node1_zk.create("/test", sequence=True)

    def get_saved_snapshots():
        return [
            obj.object_name
            for obj in list(cluster.minio_client.list_objects("snapshots"))
        ]

    def delete_s3_snapshots():
        snapshots = cluster.minio_client.list_objects("snapshots")
        for s in snapshots:
            cluster.minio_client.remove_object("snapshots", s.object_name)

    # Keeper sends snapshots asynchornously, hence we need to retry.
    def _check_snapshots():
        assert set(get_saved_snapshots()) == set(
            [
                "snapshot_50.bin.zstd",
                "snapshot_100.bin.zstd",
                "snapshot_150.bin.zstd",
                "snapshot_200.bin.zstd",
            ]
        )

    retry(AssertionError, retries=10, delay=2, jitter=0, backoff=1)(_check_snapshots)

    destroy_zk_client(node1_zk)
    node1.stop_clickhouse(kill=True)

    # wait for new leader to be picked and that it continues
    # uploading snapshots
    wait_node(node2)
    node2_zk = get_fake_zk(node2.name)
    for _ in range(200):
        node2_zk.create("/test", sequence=True)

    def _check_snapshots_without_quorum():
        assert len(get_saved_snapshots()) > 4

    retry(AssertionError, retries=10, delay=2, jitter=0, backoff=1)(
        _check_snapshots_without_quorum
    )

    _check_snapshots_without_quorum()

    success_upload_message = "Successfully uploaded"
    assert node2.contains_in_log(success_upload_message) or node3.contains_in_log(
        success_upload_message
    )

    destroy_zk_client(node2_zk)
    node2.stop_clickhouse()
    delete_keeper_snapshots_logs(node2)
    node3.stop_clickhouse()
    delete_keeper_snapshots_logs(node3)
    delete_keeper_snapshots_logs(node1)
    p = Pool(3)
    waiters = []

    def start_clickhouse(node):
        node.start_clickhouse()

    waiters.append(p.apply_async(start_clickhouse, args=(node1,)))
    waiters.append(p.apply_async(start_clickhouse, args=(node2,)))
    waiters.append(p.apply_async(start_clickhouse, args=(node3,)))

    delete_s3_snapshots()  # for next iteration

    for waiter in waiters:
        waiter.wait()

    keeper_utils.wait_until_connected(cluster, node1)
    keeper_utils.wait_until_connected(cluster, node2)
    keeper_utils.wait_until_connected(cluster, node3)
