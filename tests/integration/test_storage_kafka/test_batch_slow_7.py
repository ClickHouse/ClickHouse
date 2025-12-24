"""Long running tests, longer than 30 seconds"""

from helpers.kafka.common_direct import *
import helpers.kafka.common as k
import pandas as pd

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml", "configs/named_collection.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,  # For Kafka2
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


# Tests


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_consumer_reschedule_logging(kafka_cluster, create_query_generator):
    """
    Introspect that kafka_consumer_reschedule_ms is used to reschedule consumers.
    """
    suffix = k.random_string(6)
    kafka_table = f"kafka_reschedule_log_{suffix}"
    topic_name = f"test_reschedule_logging_{suffix}"

    test_reschedule_ms = 250

    create_query = create_query_generator(
        kafka_table,
        "key UInt64, value UInt64",
        topic_list=topic_name,
        consumer_group=topic_name,
        settings={
            "kafka_max_block_size": 100,
            "kafka_flush_interval_ms": 100,
            "kafka_consumer_reschedule_ms": test_reschedule_ms,
        },
    )

    instance.query(
        f"""
        {create_query};

        CREATE MATERIALIZED VIEW test.{kafka_table}_destination
        ENGINE=MergeTree ORDER BY tuple() AS
        SELECT key, value FROM test.{kafka_table};
    """
    )

    messages = [json.dumps({"key": j, "value": j}) for j in range(50)]
    k.kafka_produce(kafka_cluster, topic_name, messages)

    # Wait for consumption and stall
    instance.wait_for_log_line(f"{kafka_table}.*Committed offset 50")

    # Check that the log shows the correct reschedule interval
    instance.wait_for_log_line(
        f"{kafka_table}.*Rescheduling in {test_reschedule_ms} ms"
    )

    # Verify messages consumed
    result = int(instance.query(f"SELECT count() FROM test.{kafka_table}_destination"))
    assert result == 50


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_system_kafka_consumers_timestamp(kafka_cluster, create_query_generator):
    suffix = k.random_string(6)
    kafka_table = f"kafka_{suffix}"

    admin_client = KafkaAdminClient(
        bootstrap_servers=f"localhost:{kafka_cluster.kafka_port}"
    )

    topic_name = "system_kafka_consumers_timestamp" + k.get_topic_postfix(
        create_query_generator
    )

    with k.kafka_topic(admin_client, topic_name, num_partitions=6):

        k.kafka_produce(
            kafka_cluster,
            topic_name,
            ["1|foo", "2|bar", "42|answer", "100|multi\n101|row\n103|message"],
        )

        instance.query(
            f"""
            DROP TABLE IF EXISTS test.{kafka_table} SYNC;
            DROP TABLE IF EXISTS test.{kafka_table}_view SYNC;

            {create_query_generator(
                kafka_table,
                "a UInt64, b String",
                topic_list=topic_name,
                consumer_group=topic_name,
                format="CSV",
                settings={
                    "format_csv_delimiter":"|",
                    "kafka_commit_on_select": 1
                }
            )};

            CREATE MATERIALIZED VIEW test.{kafka_table}_view ENGINE=MergeTree ORDER BY tuple() AS SELECT * FROM test.{kafka_table};
            """
        )
        instance.query_with_retry(
            f"SELECT count() FROM test.{kafka_table}_view",
            check_callback=lambda res: int(res) == 4,
        )

        time.sleep(5)

        latency_array = instance.query(
            f"""
            SELECT now()::UInt64, arrayMap(x -> now() - x, assignments.produce_time), assignments.current_offset
            FROM system.kafka_consumers WHERE database='test' and table='{kafka_table}';
            """,
            parse=True,
        ).to_dict("records")[0]

        now = int(list(latency_array.values())[0])
        timestamps = pd.eval(list(latency_array.values())[1])
        offsets = pd.eval(list(latency_array.values())[2])
        logging.info(f"offsets {offsets}")
        logging.info(f"timestamps {timestamps}")

        total_offset = 0
        for i in range(len(offsets)):
            if offsets[i] > 0:
                total_offset += offsets[i]
                # non zero timestamp
                assert timestamps[i] < 100
            else:
                # no messages in this TopicPartition, timestamp is zero
                assert timestamps[i] > 100

        # We sent 4 messages
        assert total_offset == 4

        instance.query_with_retry(f"DROP TABLE test.{kafka_table}_view SYNC")
        instance.query(f"DROP TABLE test.{kafka_table}")
