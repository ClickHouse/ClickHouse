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
    with k.existing_kafka_topic(admin, topic_name):
        create_kafka = k.generate_new_create_table_query(
            table_name="kafka",
            columns_def="key UInt64, value UInt64",
            database="test",
            topic_list=topic_name,
            consumer_group=topic_name,
            keeper_path="/clickhouse/test/zk_locks",
            replica_name="r1",
            brokers="kafka1:19092"
        )
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.kafka;
            DROP TABLE IF EXISTS test.view;
            DROP TABLE IF EXISTS test.consumer;

            {create_kafka};
            CREATE TABLE test.view (key UInt64, value UInt64) ENGINE = MergeTree() ORDER BY key;
            CREATE MATERIALIZED VIEW test.consumer TO test.view AS SELECT * FROM test.kafka;
            """
        )

        for pid in range(3):
            k.kafka_produce(kafka_cluster, topic_name, [f"msg_{pid}"], retries=5)
        
        base = "/clickhouse/test/zk_locks/topic_partition_locks"
        expected_locks = {f"zk_locks_topic_{pid}.lock" for pid in range(3)}
        with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
            timeout, interval = 30.0, 1.0
            start = time.time()
            while time.time() - start < timeout:
                children = set(zk.ls(base))
                if children == expected_locks:
                    break
                time.sleep(interval)
            else:
                pytest.fail(f"Timed out waiting for locks in ZK: got {children}, expected {expected_locks}")

            for lock in expected_locks:
                owner = zk.get(f"{base}/{lock}")
                assert owner == "r1", f"Expected 'r1' in {lock}, got {owner}"

def test_two_replicas_balance(kafka_cluster):
    admin = k.get_admin_client(kafka_cluster)
    topic = "zk_dist_topic"
    k.kafka_create_topic(admin, topic, num_partitions=4)
    with k.existing_kafka_topic(admin, topic):
        create_kafka1 = k.generate_new_create_table_query(
            table_name="kafka1",
            columns_def="key UInt64, value UInt64",
            database="test",
            topic_list=topic,
            consumer_group=topic,
            keeper_path="/clickhouse/test/zk_dist",
            replica_name="r1",
            brokers="kafka1:19092"
        )
        create_kafka2 = k.generate_new_create_table_query(
            table_name="kafka2",
            columns_def="key UInt64, value UInt64",
            database="test",
            topic_list=topic,
            consumer_group=topic,
            keeper_path="/clickhouse/test/zk_dist",
            replica_name="r2",
            brokers="kafka1:19092"
        )
        instance.query(f"""
            DROP TABLE IF EXISTS test.kafka1;
            DROP TABLE IF EXISTS test.kafka2;
            DROP TABLE IF EXISTS test.view1;
            DROP TABLE IF EXISTS test.view2;
            DROP TABLE IF EXISTS test.cons1;
            DROP TABLE IF EXISTS test.cons2;

            {create_kafka1};
            {create_kafka2};
            CREATE TABLE test.view1  (key UInt64, value UInt64) ENGINE = MergeTree() ORDER BY key;
            CREATE TABLE test.view2  (key UInt64, value UInt64) ENGINE = MergeTree() ORDER BY key;

            CREATE MATERIALIZED VIEW test.cons1 TO test.view1 AS SELECT * FROM test.kafka1;
            CREATE MATERIALIZED VIEW test.cons2 TO test.view2 AS SELECT * FROM test.kafka2;
        """)

        for pid in range(4):
            k.kafka_produce(kafka_cluster, topic, [f"msg_{pid}"], retries=5)

        base = "/clickhouse/test/zk_dist/topic_partition_locks"
        expected_locks = {f"{topic}_{pid}.lock" for pid in range(4)}
        with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
            timeout, interval = 30.0, 1.0
            start = time.time()
            while time.time() - start < timeout:
                children = set(zk.ls(base))
                if children == expected_locks:
                    break
                time.sleep(interval)
            else:
                pytest.fail(f"Timed out waiting for locks: got {children!r}, expected {expected_locks!r}")
            
            counts = {"r1": 0, "r2": 0}
            for lock in expected_locks:
                owner = zk.get(f"{base}/{lock}")
                counts[owner] += 1
            valid_counts = [
                {"r1": 2, "r2": 2},
                {"r1": 3, "r2": 1},
                {"r1": 1, "r2": 3},
            ]
            assert counts in valid_counts, f"Unexpected distribution: {counts}"