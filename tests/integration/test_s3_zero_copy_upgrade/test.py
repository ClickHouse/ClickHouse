import logging
import time
import kazoo

import pytest
from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node1", main_configs=["configs/config.d/s3.xml"], macros={'replica': '1'},
                             image='yandex/clickhouse-server', tag='21.11.4.14',
                             stay_alive=True, with_installed_binary=True,
                             with_minio=True,
                             with_zookeeper=True)
        cluster.add_instance("node2", main_configs=["configs/config.d/s3.xml"], macros={'replica': '2'},
                             image='yandex/clickhouse-server', tag='21.11.4.14',
                             stay_alive=True, with_installed_binary=True,
                             with_minio=True,
                             with_zookeeper=True)
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def get_large_objects_count(cluster, size=100, folder='data'):
    minio = cluster.minio_client
    counter = 0
    for obj in minio.list_objects(cluster.minio_bucket, '{}/'.format(folder)):
        if obj.size is not None and obj.size >= size:
            counter = counter + 1
    return counter


def check_objects_exisis(cluster, object_list, folder='data'):
    minio = cluster.minio_client
    for obj in object_list:
        if obj:
            minio.stat_object(cluster.minio_bucket, '{}/{}'.format(folder, obj))


def check_objects_not_exisis(cluster, object_list, folder='data'):
    minio = cluster.minio_client
    for obj in object_list:
        if obj:
            try:
                minio.stat_object(cluster.minio_bucket, '{}/{}'.format(folder, obj))
            except Exception as error:
                assert "NoSuchKey" in str(error)
            else:
                assert False, "Object {} should not be exists".format(obj)


def wait_for_large_objects_count(cluster, expected, size=100, timeout=30):
    while timeout > 0:
        if get_large_objects_count(cluster, size=size) == expected:
            return
        timeout -= 1
        time.sleep(1)
    assert get_large_objects_count(cluster, size=size) == expected


def wait_for_count_in_table(node, table, count, seconds):
    while seconds > 0:
        seconds -= 1
        res = node.query(f"SELECT count() FROM {table}")
        if res == f"{count}\n":
            return
        time.sleep(1)
    res = node.query(f"SELECT count() FROM {table}")
    assert res == f"{count}\n"


def get_ids(zookeeper, zk_path):
    ids = []

    try:
        zk_nodes = zookeeper.get_children(zk_path)

        for zk_node in zk_nodes:
            part_ids = zookeeper.get_children(zk_path + "/" + zk_node)
            assert len(part_ids) == 1
            ids += part_ids
    except kazoo.exceptions.NoNodeError:
        ids = []
        pass

    ids = list(set(ids))
    ids.sort()
    return ids


def get_ids_new(zookeeper, zk_path):
    ids = []

    try:
        zk_tables = zookeeper.get_children(zk_path)
        for zk_table in zk_tables:
            zk_nodes = zookeeper.get_children(zk_path + "/" + zk_table)
            for zk_node in zk_nodes:
                part_ids = zookeeper.get_children(zk_path + "/" + zk_table + "/" + zk_node)
                assert len(part_ids) == 1
                ids += part_ids
    except kazoo.exceptions.NoNodeError:
        ids = []
        pass

    ids = list(set(ids))
    ids.sort()
    return ids


def wait_mutations(node, table, seconds):
    time.sleep(1)
    while seconds > 0:
        seconds -= 1
        mutations = node.query(f"SELECT count() FROM system.mutations WHERE table='{table}' AND is_done=0")
        if mutations == '0\n':
            return
        time.sleep(1)
    mutations = node.query(f"SELECT count() FROM system.mutations WHERE table='{table}' AND is_done=0")
    assert mutations == '0\n'


def test_s3_zero_copy_version_upgrade(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    zookeeper = cluster.get_kazoo_client("zoo1")

    node1.query("DROP TABLE IF EXISTS convert_test NO DELAY")
    node2.query("DROP TABLE IF EXISTS convert_test NO DELAY")

    node1.query(
        """
        CREATE TABLE convert_test ON CLUSTER test_cluster (d String)
        ENGINE=ReplicatedMergeTree('/clickhouse/tables/convert_test', '{}')
        ORDER BY d
        PARTITION BY d
        SETTINGS storage_policy='s3'
        """
            .format('{replica}')
    )

    node1.query("INSERT INTO convert_test VALUES ('convert_part_1'),('convert_part_2'),('convert_part_3')")
    wait_for_count_in_table(node2, "convert_test", 3, 10)

    zk_old_path = "/clickhouse/tables/convert_test/zero_copy_s3/shared"
    zk_path = "/clickhouse/zero_copy/zero_copy_s3"

    part_ids = get_ids(zookeeper, zk_old_path)
    assert len(part_ids) == 3

    ids = get_ids_new(zookeeper, zk_path)
    assert len(ids) == 0

    node1.restart_with_latest_version()
    ids = get_ids_new(zookeeper, zk_path)
    assert ids == part_ids
    old_ids = get_ids(zookeeper, zk_old_path)
    assert old_ids == part_ids

    node1.restart_clickhouse()
    ids = get_ids_new(zookeeper, zk_path)
    assert ids == part_ids
    old_ids = get_ids(zookeeper, zk_old_path)
    assert old_ids == part_ids

    node1.query("INSERT INTO convert_test VALUES ('convert_part_4')")
    wait_for_count_in_table(node1, "convert_test", 4, 10)
    wait_for_count_in_table(node2, "convert_test", 4, 10)
    node2.query("INSERT INTO convert_test VALUES ('convert_part_5')")
    wait_for_count_in_table(node1, "convert_test", 5, 10)
    wait_for_count_in_table(node2, "convert_test", 5, 10)

    part_ids = get_ids_new(zookeeper, zk_path)
    assert len(part_ids) == 5
    old_ids = get_ids(zookeeper, zk_old_path)
    assert old_ids == part_ids

    node1.query("ALTER TABLE convert_test DETACH PARTITION 'convert_part_1'")
    node1.query("ALTER TABLE convert_test DROP DETACHED PARTITION 'convert_part_1'", settings={"allow_drop_detached": 1})
    wait_for_count_in_table(node1, "convert_test", 4, 10)
    wait_for_count_in_table(node2, "convert_test", 4, 10)
    node2.query("ALTER TABLE convert_test DETACH PARTITION 'convert_part_2'")
    node2.query("ALTER TABLE convert_test DROP DETACHED PARTITION 'convert_part_2'", settings={"allow_drop_detached": 1})
    wait_for_count_in_table(node1, "convert_test", 3, 10)
    wait_for_count_in_table(node2, "convert_test", 3, 10)
    wait_mutations(node1, "convert_test", 10)
    wait_mutations(node2, "convert_test", 10)

    part_ids = get_ids_new(zookeeper, zk_path)
    assert len(part_ids) == 4

    node1.query("ALTER TABLE convert_test DROP DETACHED PARTITION 'convert_part_2'", settings={"allow_drop_detached": 1})
    wait_mutations(node1, "convert_test", 10)

    part_ids = get_ids_new(zookeeper, zk_path)
    assert len(part_ids) == 3

    node2.restart_with_latest_version()
    ids = get_ids_new(zookeeper, zk_path)
    assert ids == part_ids
    old_ids = get_ids(zookeeper, zk_old_path)
    assert len(old_ids) == 0

    node1.query("DROP TABLE IF EXISTS convert_test NO DELAY")
    node2.query("DROP TABLE IF EXISTS convert_test NO DELAY")

    zookeeper.stop()
