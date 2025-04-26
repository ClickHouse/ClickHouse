import time
import pytest

from helpers.kafka.common import (
    get_admin_client,
    kafka_create_topic,
    kafka_produce,
    generate_new_create_table_query,
)

from helpers.cluster import ClickHouseCluster
from helpers.keeper_utils import KeeperClient

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml", "configs/named_collection.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,
    macros={
        "kafka_broker": "kafka1",
        "kafka_topic_new": "zk_locks_topic",
        "kafka_group_name_new": "zk_locks_group",
        "kafka_client_id": "instance",
        "kafka_format_json_each_row": "JSONEachRow",
    },
    clickhouse_path_dir="clickhouse_path",
)

@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

@pytest.fixture(autouse=True)
def clean_db_and_topic(kafka_cluster):
    instance.query("DROP DATABASE IF EXISTS test SYNC; CREATE DATABASE test;")
    admin = get_admin_client(kafka_cluster)
    kafka_create_topic(admin, "zk_locks_topic", num_partitions=3)
    yield
    admin.delete_topics(["zk_locks_topic"])

def test_zookeeper_partition_locks(kafka_cluster):
    create = generate_new_create_table_query(
        table_name="zk_locks",
        columns_def="i UInt64",
        database="test",
        topic_list="zk_locks_topic",
        consumer_group="zk_locks_group",
        keeper_path="/clickhouse/test/zk_locks",
        replica_name="r1",
        settings=None,
    )
    instance.query(create)

    for pid in range(3):
        kafka_produce(kafka_cluster, "zk_locks_topic", [f"msg_{pid}"], retries=5)

    time.sleep(5)

    with KeeperClient.from_cluster(kafka_cluster, keeper_node="instance") as zk:
        base = "/clickhouse/test/zk_locks/topic_partition_locks"
        children = set(zk.ls(base))
        expected = {f"zk_locks_topic_{pid}.lock" for pid in range(3)}
        assert children == expected, f"ZK children: {children}"

        for name in expected:
            data = zk.get(f"{base}/{name}")
            assert data == "r1", f"Expected 'r1' in {name}, got {data}"