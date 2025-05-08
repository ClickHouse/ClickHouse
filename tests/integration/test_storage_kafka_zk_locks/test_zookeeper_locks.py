import time
import pytest
import logging

from . import common as k

from helpers.cluster import ClickHouseCluster
from helpers.keeper_utils import KeeperClient

cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml"],
    user_configs=[],
    with_kafka=True,
    with_zookeeper=True,
    macros={
        "kafka_broker": "kafka1",
        "kafka_topic_new": "zk_locks_topic",
        "kafka_group_name_new": "zk_locks_group",
        "kafka_client_id": "instance",
        "kafka_format_json_each_row": "JSONEachRow",
    }
)

@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    instance.query("DROP DATABASE IF EXISTS test SYNC; CREATE DATABASE test;")
    admin_client = k.get_admin_client(cluster)

    def get_topics_to_delete():
        return [t for t in admin_client.list_topics() if not t.startswith("_")]

    topics = get_topics_to_delete()
    logging.debug(f"Deleting topics: {topics}")
    result = admin_client.delete_topics(topics)
    for topic, error in result.topic_error_codes:
        if error != 0:
            logging.warning(f"Received error {error} while deleting topic {topic}")
        else:
            logging.info(f"Deleted topic {topic}")

    retries = 0
    topics = get_topics_to_delete()
    while len(topics) != 0:
        logging.info(f"Existing topics: {topics}")
        if retries >= 5:
            raise Exception(f"Failed to delete topics {topics}")
        retries += 1
        time.sleep(0.5)
    yield  # run test

def test_zookeeper_partition_locks(kafka_cluster):
    admin = k.get_admin_client(kafka_cluster)
    topic_name = "zk_locks_topic"
    k.kafka_create_topic(admin, "zk_locks_topic", num_partitions=3)
    instance.query(
        f"""
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic_name}',
                     kafka_group_name = '{topic_name}',
                     kafka_format = 'JSONEachRow',
                     kafka_keeper_path = '/clickhouse/test/zk_locks',
                     kafka_replica_name = 'r1',
                     allow_experimental_kafka_offsets_storage_in_keeper=1;
        CREATE TABLE test.view (key UInt64, value UInt64) ENGINE = MergeTree() ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS SELECT * FROM test.kafka;
        """
    )

    for pid in range(3):
        k.kafka_produce(kafka_cluster, "zk_locks_topic", [f"msg_{pid}"], retries=5)

    time.sleep(30)

    with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
        base = "/clickhouse/test/zk_locks/topic_partition_locks"
        children = set(zk.ls(base))
        expected = {f"zk_locks_topic_{pid}.lock" for pid in range(3)}
        assert children == expected, f"ZK children: {children}"

        for name in expected:
            data = zk.get(f"{base}/{name}")
            assert data == "r1", f"Expected 'r1' in {name}, got {data}"
    
    k.kafka_delete_topic(admin, topic_name)
