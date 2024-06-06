import pytest
import random
import string
import threading

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

NUM_WORKERS = 5

MAX_ROWS = 1000

dirs_created = []


def gen_insert_values(size):
    return ",".join(
        f"({i},'{''.join(random.choices(string.ascii_lowercase, k=5))}')"
        for i in range(size)
    )


insert_values = ",".join(
    f"({i},'{''.join(random.choices(string.ascii_lowercase, k=5))}')" for i in range(10)
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    for i in range(NUM_WORKERS):
        cluster.add_instance(
            f"node{i + 1}",
            main_configs=["configs/storage_conf.xml"],
            with_minio=True,
            env_variables={"ENDPOINT_SUBPATH": f"node{i + 1}"},
            stay_alive=True,
            # Override ENDPOINT_SUBPATH.
            instance_env_variables=i > 0,
        )

    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.order(0)
def test_insert():
    def create_insert(node, insert_values):
        node.query(
            """
            CREATE TABLE test (
                id Int64,
                data String
            ) ENGINE=MergeTree()
            ORDER BY id
            SETTINGS storage_policy='s3_plain_rewritable'
            """
        )
        node.query("INSERT INTO test VALUES {}".format(insert_values))

    insert_values_arr = [
        gen_insert_values(random.randint(1, MAX_ROWS)) for _ in range(0, NUM_WORKERS)
    ]
    threads = []
    assert len(cluster.instances) == NUM_WORKERS
    for i in range(NUM_WORKERS):
        node = cluster.instances[f"node{i + 1}"]
        t = threading.Thread(target=create_insert, args=(node, insert_values_arr[i]))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    for i in range(NUM_WORKERS):
        node = cluster.instances[f"node{i + 1}"]
        assert (
            node.query("SELECT * FROM test ORDER BY id FORMAT Values")
            == insert_values_arr[i]
        )

    for i in range(NUM_WORKERS):
        node = cluster.instances[f"node{i + 1}"]
        node.query("ALTER TABLE test MODIFY SETTING old_parts_lifetime = 59")
        assert (
            node.query(
                "SELECT engine_full from system.tables WHERE database = currentDatabase() AND name = 'test'"
            ).find("old_parts_lifetime = 59")
            != -1
        )

        node.query("ALTER TABLE test RESET SETTING old_parts_lifetime")
        assert (
            node.query(
                "SELECT engine_full from system.tables WHERE database = currentDatabase() AND name = 'test'"
            ).find("old_parts_lifetime")
            == -1
        )
        node.query("ALTER TABLE test MODIFY COMMENT 'new description'")
        assert (
            node.query(
                "SELECT comment from system.tables WHERE database = currentDatabase() AND name = 'test'"
            ).find("new description")
            != -1
        )

        created = int(
            node.query(
                "SELECT value FROM system.events WHERE event = 'DiskPlainRewritableS3DirectoryCreated'"
            )
        )
        assert created > 0
        dirs_created.append(created)
        assert (
            int(
                node.query(
                    "SELECT value FROM system.metrics WHERE metric = 'DiskPlainRewritableS3DirectoryMapSize'"
                )
            )
            == created
        )


@pytest.mark.order(1)
def test_restart():
    insert_values_arr = []
    for i in range(NUM_WORKERS):
        node = cluster.instances[f"node{i + 1}"]
        insert_values_arr.append(
            node.query("SELECT * FROM test ORDER BY id FORMAT Values")
        )

    def restart(node):
        node.restart_clickhouse()

    threads = []
    for i in range(NUM_WORKERS):
        t = threading.Thread(target=restart, args=(node,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    for i in range(NUM_WORKERS):
        node = cluster.instances[f"node{i + 1}"]
        assert (
            node.query("SELECT * FROM test ORDER BY id FORMAT Values")
            == insert_values_arr[i]
        )


@pytest.mark.order(2)
def test_drop():
    for i in range(NUM_WORKERS):
        node = cluster.instances[f"node{i + 1}"]
        node.query("DROP TABLE IF EXISTS test SYNC")

        removed = int(
            node.query(
                "SELECT value FROM system.events WHERE event = 'DiskPlainRewritableS3DirectoryRemoved'"
            )
        )

        assert dirs_created[i] == removed

    it = cluster.minio_client.list_objects(
        cluster.minio_bucket, "data/", recursive=True
    )

    assert len(list(it)) == 0
