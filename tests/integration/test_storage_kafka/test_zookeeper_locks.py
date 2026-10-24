from collections import Counter
import json
import time
import pytest
import logging

from helpers.kafka.common_direct import *
import helpers.kafka.common as k

from helpers.keeper_utils import KeeperClient

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka_and_keeper.xml"],
    user_configs=["configs/users.xml"],
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


# Fixtures
@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        kafka_id = instance.cluster.kafka_docker_id
        print(("kafka_id is {}".format(kafka_id)))
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


# Tests


def test_zookeeper_partition_locks(kafka_cluster):
    admin = k.get_admin_client(kafka_cluster)
    num_partitions = 3
    topic_name = "zk_locks_topic"
    keeper_path = "/clickhouse/test/zk_locks"

    k.kafka_create_topic(admin, "zk_locks_topic", num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        create_kafka = k.generate_new_create_table_query(
            table_name="kafka",
            columns_def="key UInt64, value UInt64",
            database="test",
            topic_list=topic_name,
            consumer_group=topic_name,
            keeper_path=keeper_path,
            replica_name="r1"
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

        messages = []
        for i in range(num_partitions):
            messages.append(json.dumps({"key": i, "value": i}))
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)

        base = f"{keeper_path}/topic_partition_locks"
        expected_locks = {f"zk_locks_topic_{pid}.lock" for pid in range(num_partitions)}
        with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
            start = time.time()
            timeout, interval = 30.0, 1.0
            while time.time() - start < timeout:
                children = set(zk.ls(base))
                if children == expected_locks:
                    break
                time.sleep(interval)
            else:
                pytest.fail(f"Timed out waiting for locks in ZK: got {children!r}, expected {expected_locks!r}")

            for lock in expected_locks:
                owner = zk.get(f"{base}/{lock}")
                assert owner == "r1", f"Expected 'r1' in {lock}, got {owner}"


def test_three_replicas_ten_partitions_rebalance(kafka_cluster):
    admin = k.get_admin_client(kafka_cluster)
    topic_name= "zk_dist_topic_10p"
    num_partitions = 10
    replica_names = ["r1", "r2", "r3"]
    keeper_path = "/clickhouse/test/zk_dist3"

    k.kafka_create_topic(admin, topic_name, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic_name):
        create_kafka_queries = [
            k.generate_new_create_table_query(
                table_name=f"kafka_{replica}",
                columns_def="key UInt64, value UInt64",
                database="test",
                topic_list=topic_name,
                consumer_group=topic_name,
                keeper_path=keeper_path,
                replica_name=replica
            ) + ";"
            for replica in replica_names
        ]
        view_queries = [
            f"CREATE TABLE test.view_{replica} (key UInt64, value UInt64) ENGINE = MergeTree() ORDER BY key;"
            for replica in replica_names
        ]
        mv_queries = [
            f"CREATE MATERIALIZED VIEW test.cons_{replica} TO test.view_{replica} AS SELECT * FROM test.kafka_{replica};"
            for replica in replica_names
        ]
        drop_queries = [
            f"DROP TABLE IF EXISTS test.kafka_{replica};"
            f"DROP TABLE IF EXISTS test.view_{replica};"
            f"DROP TABLE IF EXISTS test.cons_{replica};"
            for replica in replica_names
        ]
        instance.query(
            "\n".join(drop_queries) + "\n" +
            "\n".join(create_kafka_queries) + "\n" +
            "\n".join(view_queries) + "\n" +
            "\n".join(mv_queries)
        )

        messages = []
        for i in range(num_partitions):
            messages.append(json.dumps({"key": i, "value": i}))
        k.kafka_produce(kafka_cluster, topic_name, messages, retries=5)

        base = f"{keeper_path}/topic_partition_locks"
        expected_locks = {f"{topic_name}_{pid}.lock" for pid in range(num_partitions)}
        with KeeperClient.from_cluster(kafka_cluster, keeper_node="zoo1") as zk:
            start = time.time()
            timeout, interval = 30.0, 1.0
            while time.time() - start < timeout:
                children = set(zk.ls(base))
                if children == expected_locks:
                    break
                time.sleep(interval)
            else:
                pytest.fail(f"Timed out waiting for locks in ZK: got {children!r}, expected {expected_locks!r}")

            counts = {replica: 0 for replica in replica_names}
            for lock in expected_locks:
                owner = zk.get(f"{base}/{lock}")
                if owner not in counts:
                    pytest.fail(f"Unknown owner {owner!r} for lock {lock}")
                counts[owner] += 1

            base_count = num_partitions // len(replica_names)
            values = sorted(counts.values())
            assert sum(values) == num_partitions
            assert all(v in (base_count-1, base_count, base_count+1) for v in values), f"Values: {values}"
            assert values[-1] - values[0] <= 2
