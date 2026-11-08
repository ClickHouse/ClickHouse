import logging

from helpers.kafka.common_direct import *
from helpers.kafka.common_direct import _VarintBytes
import helpers.kafka.common as k


# protoc --version
# libprotoc 3.0.0
# # to create kafka_pb2.py
# protoc --python_out=. kafka.proto


# TODO: add test for run-time offset update in CH, if we manually update it on Kafka side.
# TODO: add test for SELECT LIMIT is working.


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml", "configs/named_collection.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,  # For Replicated Table
    macros={
        "kafka_broker": "kafka1",
        "kafka_topic_old": k.KAFKA_TOPIC_OLD,
        "kafka_group_name_old": k.KAFKA_CONSUMER_GROUP_OLD,
        "kafka_topic_new": k.KAFKA_TOPIC_NEW,
        "kafka_group_name_new": k.KAFKA_CONSUMER_GROUP_NEW,
        "kafka_client_id": "instance",
        "kafka_format_json_each_row": "JSONEachRow",
    },
    clickhouse_path_dir="clickhouse_path",
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


def test_good_intent_size(kafka_cluster):
    instance.rotate_logs()

    topic_name = "topic_intent_size"

    def produce_message(index):
        k.kafka_produce(
            kafka_cluster,
            topic_name,
            [f"message_{index}"]
        )

    produce_message(1)

    create_kafka_query = k.generate_new_create_table_query("kafka", "a String", topic_list=topic_name, format="LineAsString", settings={"kafka_flush_interval_ms": 3000})

    with k.existing_kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        instance.query(
            f"""
            CREATE TABLE test.dst (
                a String,
            )
            ENGINE = MergeTree()
            ORDER BY a;

            {create_kafka_query};

            CREATE MATERIALIZED VIEW test.kafka_mv TO test.dst AS
            SELECT
                a
            FROM test.kafka;
            """
        )

        def check_intent_size(num_messages, offset):
            consumed_messages = instance.query_with_retry("SELECT * FROM test.dst", retry_count =30, sleep_time=1, check_callback=lambda x: len(TSV(x)) == num_messages)
            logging.debug(f"Consumed messages: {consumed_messages}")
            instance.wait_for_log_line(f"Saving intent of 1 for topic-partition \\[{topic_name}:0\\] at offset {offset}")

        INVALID_KAFKA_OFFSET = -1001
        check_intent_size(1, INVALID_KAFKA_OFFSET)

        produce_message(2)
        check_intent_size(2, 1)

        result = instance.query("SELECT * FROM test.dst ORDER BY a")
        assert TSV(result) == TSV("message_1\nmessage_2\n")

        instance.query(
            """
            DROP TABLE test.kafka SYNC;
            TRUNCATE TABLE test.dst SETTINGS alter_sync=1;
            """)
        instance.query(create_kafka_query)

        # Check that intent size is also correct when we have no saved committed offset
        produce_message(3)
        check_intent_size(1, INVALID_KAFKA_OFFSET)
        # Do an extra check to make sure wait_for_log_line in `check_intent_size` didn't caught the wrong line
        assert instance.wait_for_log_line(f"Saving intent of 1 for topic-partition \\[{topic_name}:0\\] at offset {INVALID_KAFKA_OFFSET}", repetitions=2)

        # Check that intent size is correct with multiple messages
        k.kafka_produce(
            kafka_cluster,
            topic_name,
            [f"message_4", "message_5"]
        )

        consumed_messages = instance.query_with_retry("SELECT * FROM test.dst", retry_count = 30, sleep_time = 1, check_callback=lambda x: len(TSV(x)) == 3)
        logging.debug(f"Consumed messages: {consumed_messages}")
        instance.wait_for_log_line(f"Saving intent of 2 for topic-partition \\[{topic_name}:0\\] at offset 3")

        result = instance.query("SELECT * FROM test.dst ORDER BY a")
        assert TSV(result) == TSV("message_3\nmessage_4\nmessage_5")


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
