"""Long running tests, longer than 30 seconds"""

from helpers.kafka.common_direct import *
import helpers.kafka.common as k

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


# Tests


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_bad_reschedule(kafka_cluster, create_query_generator):
    suffix = k.random_string(6)
    kafka_table = f"kafka_{suffix}"

    topic_name = "test_bad_reschedule" + k.get_topic_postfix(create_query_generator)

    messages = [json.dumps({"key": j + 1, "value": j + 1}) for j in range(20000)]
    k.kafka_produce(kafka_cluster, topic_name, messages)

    create_query = create_query_generator(
        kafka_table,
        "key UInt64, value UInt64",
        topic_list=topic_name,
        consumer_group=topic_name,
        settings={
            "kafka_max_block_size": 1000,
            "kafka_flush_interval_ms": 1000,
        },
    )
    instance.query(
        f"""
        {create_query};

        CREATE MATERIALIZED VIEW test.{kafka_table}_destination ENGINE=MergeTree ORDER BY tuple() AS
        SELECT
            key,
            now() as consume_ts,
            value,
            _topic,
            _key,
            _offset,
            _partition,
            _timestamp
        FROM test.{kafka_table};
    """
    )

    instance.wait_for_log_line(f"{kafka_table}.*Committed offset 20000")

    logging.debug("Timestamps: %s", instance.query(f"SELECT max(consume_ts), min(consume_ts) FROM test.{kafka_table}_destination"))
    assert int(instance.query(f"SELECT max(consume_ts) - min(consume_ts) FROM test.{kafka_table}_destination")) < 8


@pytest.mark.parametrize(
    "create_query_generator, do_direct_read",
    [
        (k.generate_old_create_table_query, True),
        (k.generate_new_create_table_query, False),
    ],
)
def test_kafka_unavailable(kafka_cluster, create_query_generator, do_direct_read):
    suffix = k.random_string(6)
    kafka_table = f"kafka_unavailable_{suffix}"

    number_of_messages = 20000
    topic_name = "test_kafka_unavailable" + k.get_topic_postfix(create_query_generator)
    messages = [
        json.dumps({"key": j + 1, "value": j + 1}) for j in range(number_of_messages)
    ]
    k.kafka_produce(kafka_cluster, topic_name, messages)

    with k.existing_kafka_topic(k.get_admin_client(kafka_cluster), topic_name):

        with kafka_cluster.pause_container("kafka1"):
            create_query = create_query_generator(
                kafka_table,
                "key UInt64, value UInt64",
                topic_list=topic_name,
                consumer_group=topic_name,
                settings={"kafka_max_block_size": 1000},
            )
            instance.query(create_query)

            # First read from the table in case we should, to make sure it doesn't crash when the broker is unavailable
            if do_direct_read:
                instance.query(f"SELECT * FROM test.{kafka_table}")

            instance.query(f"""
                CREATE MATERIALIZED VIEW test.{kafka_table}_destination ENGINE=MergeTree ORDER BY tuple() AS
                SELECT
                    key,
                    now() as consume_ts,
                    value,
                    _topic,
                    _key,
                    _offset,
                    _partition,
                    _timestamp
                FROM test.{kafka_table}
            """)
            instance.query(f"SELECT count() FROM test.{kafka_table}_destination")

            # enough to trigger issue
            time.sleep(30)

        result = instance.query_with_retry(
            f"SELECT count() FROM test.{kafka_table}_destination",
            sleep_time=1,
            check_callback=lambda res: int(res) == number_of_messages,
        )

        assert int(result) == number_of_messages
