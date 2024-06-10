import pytest
import random
import string

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage_conf.xml"],
    with_minio=True,
    stay_alive=True,
)

insert_values = [
    "(0,'data'),(1,'data')",
    ",".join(
        f"({i},'{''.join(random.choices(string.ascii_lowercase, k=5))}')"
        for i in range(10)
    ),
]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.order(0)
def test_insert():
    for index, value in enumerate(insert_values):
        node.query(
            """
            CREATE TABLE test_{} (
                id Int64,
                data String
            ) ENGINE=MergeTree()
            ORDER BY id
            SETTINGS storage_policy='s3_plain_rewritable'
            """.format(
                index
            )
        )

        node.query("INSERT INTO test_{} VALUES {}".format(index, value))
        assert (
            node.query("SELECT * FROM test_{} ORDER BY id FORMAT Values".format(index))
            == value
        )


@pytest.mark.order(1)
def test_restart():
    for index, value in enumerate(insert_values):
        assert (
            node.query("SELECT * FROM test_{} ORDER BY id FORMAT Values".format(index))
            == value
        )
    node.restart_clickhouse()

    for index, value in enumerate(insert_values):
        assert (
            node.query("SELECT * FROM test_{} ORDER BY id FORMAT Values".format(index))
            == value
        )


@pytest.mark.order(2)
def test_drop():
    for index, value in enumerate(insert_values):
        node.query("DROP TABLE IF EXISTS test_{} SYNC".format(index))

    it = cluster.minio_client.list_objects(
        cluster.minio_bucket, "data/", recursive=True
    )

    assert len(list(it)) == 0
