"""Test Kafka poll timeout after assignment recovery."""

from helpers.kafka.common_direct import *
import helpers.kafka.common as k

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml", "configs/named_collection.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,
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

    topics = [t for t in admin_client.list_topics() if not t.startswith("_")]
    logging.debug(f"Deleting topics: {topics}")
    result = admin_client.delete_topics(topics)
    for topic, error in result.topic_error_codes:
        if error != 0:
            logging.warning(f"Received error {error} while deleting topic {topic}")
        else:
            logging.info(f"Deleted topic {topic}")

    yield


def test_kafka_poll_timeout_restored_after_assignment(kafka_cluster):
    suffix = k.random_string(6)
    kafka_table = f"kafka_assignment_timeout_{suffix}"
    topic_name = f"kafka_assignment_timeout_{suffix}"
    poll_timeout_ms = 1000

    admin_client = k.get_admin_client(kafka_cluster)

    with k.kafka_topic(admin_client, topic_name):
        try:
            instance.query(
                f"""
                CREATE TABLE test.{kafka_table} (key UInt64, value UInt64)
                    ENGINE = Kafka
                    SETTINGS kafka_broker_list = 'kafka1:19092',
                             kafka_topic_list = '{topic_name}',
                             kafka_group_name = '{topic_name}',
                             kafka_format = 'JSONEachRow',
                             kafka_commit_on_select = 1,
                             kafka_poll_timeout_ms = {poll_timeout_ms};
                """
            )

            with kafka_cluster.pause_container("kafka1"):
                instance.query(f"SELECT count() FROM test.{kafka_table}", timeout=40)
                instance.wait_for_log_line(
                    f"{kafka_table}.*Can't get assignment\\. Will keep trying\\.",
                    timeout=5,
                )

            k.kafka_produce(
                kafka_cluster,
                topic_name,
                [json.dumps({"key": 1, "value": 1})],
            )

            result = instance.query_with_retry(
                f"SELECT count() FROM test.{kafka_table}",
                timeout=30,
                sleep_time=1,
                check_callback=lambda res: int(res) == 1,
            )
            assert int(result) == 1

            # Verify that the consumer recovered and reached Kafka assignment callback.
            assert_eq_with_retry(
                instance,
                f"""
                SELECT count()
                FROM system.kafka_consumers
                WHERE database = 'test'
                  AND table = '{kafka_table}'
                  AND num_rebalance_assignments >= 1
                """,
                "1\n",
            )

            # An empty direct SELECT retries several empty polls before returning.
            # If assignment recovery leaves the consumer on the 50ms fallback, this
            # finishes quickly. With the restored configured timeout it takes seconds.
            start_time = time.monotonic()
            result = instance.query(f"SELECT count() FROM test.{kafka_table}", timeout=20)
            elapsed = time.monotonic() - start_time
            logging.debug("Empty Kafka SELECT after assignment took %.3f seconds", elapsed)

            assert int(result) == 0
            # Empty direct SELECT retries 10 empty polls before returning. Half of
            # that expected time is enough to distinguish 1000ms polls from 50ms
            # fallback polls without requiring an exact 10 second runtime.
            assert elapsed >= poll_timeout_ms / 1000 * 5, elapsed
        finally:
            instance.query(f"DROP TABLE IF EXISTS test.{kafka_table} SYNC")
