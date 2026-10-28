import logging


import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
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
    user_configs=["configs/users.xml", "configs/users_mv_dedup.xml"],
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
    k.clean_test_database_and_topics(instance, cluster)
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

        # Check that intent size is correct with multiple messages.
        # Both records must already be in the topic when the batching cycle starts,
        # otherwise the cycle's flush window can expire between them and each is
        # consumed alone.
        instance.query("SYSTEM STOP test.kafka")
        k.kafka_produce(
            kafka_cluster,
            topic_name,
            ["message_4", "message_5"]
        )
        instance.query("SYSTEM START test.kafka")

        consumed_messages = instance.query_with_retry("SELECT * FROM test.dst", retry_count = 30, sleep_time = 1, check_callback=lambda x: len(TSV(x)) == 3)
        logging.debug(f"Consumed messages: {consumed_messages}")
        instance.wait_for_log_line(f"Saving intent of 2 for topic-partition \\[{topic_name}:0\\] at offset 3")

        result = instance.query("SELECT * FROM test.dst ORDER BY a")
        assert TSV(result) == TSV("message_3\nmessage_4\nmessage_5")


def test_intent_replay_does_not_over_read(kafka_cluster):
    # When an insert fails after the intent was saved, the retry must consume exactly the intended
    # number of messages, even if more messages arrived in the meantime. Otherwise the retried batch
    # would differ from the failed one, its block hash would not match, and deduplication could not
    # drop the rows that the failed insert already committed.
    instance.rotate_logs()

    topic_name = "topic_intent_replay"

    create_kafka_query = k.generate_new_create_table_query(
        "kafka",
        "a String",
        topic_list=topic_name,
        format="LineAsString",
        settings={"kafka_flush_interval_ms": 2000},
    )

    k.kafka_produce(kafka_cluster, topic_name, [f"message_{i}" for i in range(1, 4)])

    with k.existing_kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        # The first failpoint makes the Keeper commit of the first insert succeed, but report a
        # hardware fault; the second one fails the recovery from the fault, and with
        # `insert_keeper_max_retries` = 0 (set in the users config) there is no retry either. The
        # insert reports `UNKNOWN_STATUS_OF_INSERT` while its data stays committed. This mimics an
        # insert that succeeded on the server, but whose confirmation was lost, e.g. a failed
        # quorum check.
        instance.query(
            "SYSTEM ENABLE FAILPOINT replicated_merge_tree_commit_zk_fail_after_op"
        )
        instance.query(
            "SYSTEM ENABLE FAILPOINT replicated_merge_tree_commit_zk_fail_when_recovering_from_hw_fault"
        )
        try:
            instance.query(
                f"""
                CREATE TABLE test.dst (
                    a String,
                )
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/dst_intent_replay', 'r1')
                ORDER BY a;

                {create_kafka_query};

                CREATE MATERIALIZED VIEW test.kafka_mv TO test.dst AS
                SELECT
                    a
                FROM test.kafka;
                """
            )

            # The first batch seals with 3 messages when the flush interval expires. Its insert
            # commits the rows, but reports a failure, so the committed offset stays behind.
            instance.wait_for_log_line(
                f"Saving intent of 3 for topic-partition \\[{topic_name}:0\\]"
            )

            # New messages arrive between the failed attempt and the retry. The retry must replay
            # only the 3 intended messages, so that deduplication drops the block that the failed
            # insert already committed. The new messages must go into a later batch.
            k.kafka_produce(
                kafka_cluster, topic_name, [f"message_{i}" for i in range(4, 11)]
            )

            instance.query_with_retry(
                "SELECT count() FROM test.dst",
                retry_count=60,
                sleep_time=1,
                check_callback=lambda x: int(x) >= 10,
            )

            expected = sorted(f"message_{i}" for i in range(1, 11))
            result = instance.query("SELECT a FROM test.dst ORDER BY a")
            assert TSV(result) == TSV("\n".join(expected))
        finally:
            instance.query(
                "SYSTEM DISABLE FAILPOINT replicated_merge_tree_commit_zk_fail_when_recovering_from_hw_fault"
            )
            instance.query(
                "SYSTEM DISABLE FAILPOINT replicated_merge_tree_commit_zk_fail_after_op"
            )


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
