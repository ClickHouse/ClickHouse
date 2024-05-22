import pytest
import random
import string
import threading

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

NUM_WORKERS = 5

nodes = []
for i in range(NUM_WORKERS):
    name = "node{}".format(i + 1)
    node = cluster.add_instance(
        name,
        main_configs=["configs/storage_conf.xml"],
        env_variables={"ENDPOINT_SUBPATH": name},
        with_minio=True,
        stay_alive=True,
    )
    nodes.append(node)

MAX_ROWS = 1000


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
    for i in range(NUM_WORKERS):
        t = threading.Thread(
            target=create_insert, args=(nodes[i], insert_values_arr[i])
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    for i in range(NUM_WORKERS):
        assert (
            nodes[i].query("SELECT * FROM test ORDER BY id FORMAT Values")
            == insert_values_arr[i]
        )

    for i in range(NUM_WORKERS):
        nodes[i].query("ALTER TABLE test MODIFY SETTING old_parts_lifetime = 59")
        assert (
            nodes[i]
            .query(
                "SELECT engine_full from system.tables WHERE database = currentDatabase() AND name = 'test'"
            )
            .find("old_parts_lifetime = 59")
            != -1
        )

        nodes[i].query("ALTER TABLE test RESET SETTING old_parts_lifetime")
        assert (
            nodes[i]
            .query(
                "SELECT engine_full from system.tables WHERE database = currentDatabase() AND name = 'test'"
            )
            .find("old_parts_lifetime")
            == -1
        )
        nodes[i].query("ALTER TABLE test MODIFY COMMENT 'new description'")
        assert (
            nodes[i]
            .query(
                "SELECT comment from system.tables WHERE database = currentDatabase() AND name = 'test'"
            )
            .find("new description")
            != -1
        )


@pytest.mark.order(1)
def test_restart():
    insert_values_arr = []
    for i in range(NUM_WORKERS):
        insert_values_arr.append(
            nodes[i].query("SELECT * FROM test ORDER BY id FORMAT Values")
        )

    def restart(node):
        node.restart_clickhouse()

    threads = []
    for i in range(NUM_WORKERS):
        t = threading.Thread(target=restart, args=(nodes[i],))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    for i in range(NUM_WORKERS):
        assert (
            nodes[i].query("SELECT * FROM test ORDER BY id FORMAT Values")
            == insert_values_arr[i]
        )


@pytest.mark.order(2)
def test_drop():
    for i in range(NUM_WORKERS):
        nodes[i].query("DROP TABLE IF EXISTS test SYNC")

    it = cluster.minio_client.list_objects(
        cluster.minio_bucket, "data/", recursive=True
    )

    assert len(list(it)) == 0
