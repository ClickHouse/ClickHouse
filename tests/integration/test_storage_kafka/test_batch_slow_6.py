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


def test_kafka_handling_commit_failure(kafka_cluster):
    suffix = k.random_string(6)
    kafka_table = f"kafka_{suffix}"

    messages = [json.dumps({"key": j + 1, "value": "x" * 300}) for j in range(22)]
    k.kafka_produce(kafka_cluster, "handling_commit_failure", messages)

    instance.query(f"""
        DROP TABLE IF EXISTS test.{kafka_table}_view SYNC;
        DROP TABLE IF EXISTS test.{kafka_table}_consumer SYNC;

        CREATE TABLE test.{kafka_table} (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'handling_commit_failure',
                     kafka_group_name = 'handling_commit_failure',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 20,
                     kafka_flush_interval_ms = 1000;

        CREATE TABLE test.{kafka_table}_view (key UInt64, value String)
            ENGINE = MergeTree()
            ORDER BY key;
    """)

    instance.query(f"""
        CREATE MATERIALIZED VIEW test.{kafka_table}_consumer TO test.{kafka_table}_view AS
            SELECT * FROM test.{kafka_table}
            WHERE NOT sleepEachRow(0.25);
    """
    )

    instance.wait_for_log_line(f"{kafka_table}.*Polled batch of 20 messages")
    # the tricky part here is that disconnect should happen after write prefix, but before we do commit
    # we have 0.25 (sleepEachRow) * 20 ( Rows ) = 5 sec window after "Polled batch of 20 messages"
    # while materialized view is working to inject zookeeper failure

    with kafka_cluster.pause_container("kafka1"):
        instance.wait_for_log_line("timeout", timeout=60)

    # kafka_cluster.open_bash_shell('instance')
    instance.wait_for_log_line(f"{kafka_table}.*Committed offset 22")

    uniq_and_max = instance.query(f"SELECT uniqExact(key), max(key) FROM test.{kafka_table}_view")
    count = instance.query(f"SELECT count() FROM test.{kafka_table}_view")
    logging.debug(uniq_and_max)
    logging.debug(count)

    instance.query(f"""
        DROP TABLE test.{kafka_table}_consumer SYNC;
        DROP TABLE test.{kafka_table}_view SYNC;
    """)

    # After https://github.com/edenhill/librdkafka/issues/2631
    # timeout triggers rebalance, making further commits to the topic after getting back online
    # impossible. So we have a duplicate in that scenario, but we report that situation properly.
    # It is okay to have duplicates in case of commit failure, the important thing to test is we
    # each message at least once.
    assert TSV(uniq_and_max) == TSV("22\t22")
    assert int(count) >= 22


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_block_based_formats_2(kafka_cluster, create_query_generator):
    admin_client = k.get_admin_client(kafka_cluster)
    num_rows = 100
    message_count = 9

    for format_name in [
        "JSONColumns",
        "Native",
        "Arrow",
        "Parquet",
        "ORC",
        "JSONCompactColumns",
    ]:
        suffix = k.random_string(6)
        topic_name = format_name + k.get_topic_postfix(create_query_generator)
        table_name = f"kafka_{format_name}_{suffix}"
        logging.debug(f"Checking format {format_name}")
        with k.kafka_topic(admin_client, topic_name):
            create_query = create_query_generator(
                table_name,
                "key UInt64, value UInt64",
                topic_list=topic_name,
                consumer_group=topic_name,
                format=format_name,
            )

            instance.query(f"""
                DROP TABLE IF EXISTS test.{table_name}_view;
                DROP TABLE IF EXISTS test.{table_name};

                {create_query};

                CREATE MATERIALIZED VIEW test.{table_name}_view ENGINE=MergeTree ORDER BY (key, value) AS
                    SELECT key, value FROM test.{table_name};

                INSERT INTO test.{table_name}
                    SELECT number * 10 as key, number * 100 as value FROM numbers({num_rows})
                    SETTINGS max_block_size=12, optimize_trivial_insert_select=0;
            """)
            messages = k.kafka_consume_with_retry(
                kafka_cluster, topic_name, message_count, need_decode=False
            )
            assert len(messages) == message_count

            rows = int(
                instance.query_with_retry(
                    f"SELECT count() FROM test.{table_name}_view",
                    check_callback=lambda res: int(res) == num_rows,
                )
            )

            assert rows == num_rows

            result = instance.query(f"SELECT * FROM test.{table_name}_view ORDER by key")
            expected = ""
            for i in range(num_rows):
                expected += str(i * 10) + "\t" + str(i * 100) + "\n"
            assert result == expected


@pytest.mark.parametrize(
    "create_query_generator, log_line",
    [
        (k.generate_old_create_table_query, "{}.*Polled offset [0-9]+"),
        (k.generate_new_create_table_query, "{}.*Saved offset"),
    ],
)
def test_kafka_rebalance(kafka_cluster, create_query_generator, log_line):
    NUMBER_OF_CONCURRENT_CONSUMERS = 5

    suffix = k.random_string(6)

    instance.query(f"""
        DROP TABLE IF EXISTS test.kafka_destination_{suffix};
        CREATE TABLE test.kafka_destination_{suffix}
        (
            key UInt64,
            value UInt64,
            _topic String,
            _key String,
            _offset UInt64,
            _partition UInt64,
            _timestamp Nullable(DateTime('UTC')),
            _consumed_by LowCardinality(String)
        )
        ENGINE = MergeTree()
        ORDER BY key;
    """)

    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )
    topic_name = "topic_with_multiple_partitions" + k.get_topic_postfix(
        create_query_generator
    )
    table_name_prefix = "kafka_consumer"
    keeper_path = f"/clickhouse/{{database}}/{table_name_prefix}"
    with k.kafka_topic(admin_client, topic_name, num_partitions=11):
        cancel = threading.Event()

        msg_index = [0]

        def produce():
            while not cancel.is_set():
                messages = []
                for _ in range(3 * NUMBER_OF_CONCURRENT_CONSUMERS):
                    messages.append(
                        json.dumps({"key": msg_index[0], "value": msg_index[0]})
                    )
                    msg_index[0] += 1
                k.kafka_produce(kafka_cluster, topic_name, messages)

                time.sleep(0.5)

        kafka_thread = threading.Thread(target=produce)
        kafka_thread.start()

        for consumer_index in range(NUMBER_OF_CONCURRENT_CONSUMERS):
            table_name = f"{table_name_prefix}_{suffix}_{consumer_index}"
            replica_name = f"r{consumer_index}"
            logging.debug(f"Setting up {consumer_index}")

            create_query = create_query_generator(
                table_name,
                "key UInt64, value UInt64",
                topic_list=topic_name,
                keeper_path=keeper_path,
                replica_name=replica_name,
                settings={
                    "kafka_max_block_size": 33,
                    "kafka_flush_interval_ms": 500,
                },
            )
            instance.query(
                f"""
                DROP TABLE IF EXISTS test.{table_name};
                DROP TABLE IF EXISTS test.{table_name}_mv;
                {create_query};
                CREATE MATERIALIZED VIEW test.{table_name}_mv TO test.kafka_destination_{suffix} AS
                    SELECT
                    key,
                    value,
                    _topic,
                    _key,
                    _offset,
                    _partition,
                    _timestamp,
                    '{table_name}' as _consumed_by
                FROM test.{table_name};
            """
            )
            instance.wait_for_log_line(log_line.format(table_name))

            logging.debug(instance.query(f"SELECT count(), uniqExact(key), max(key) + 1 FROM test.kafka_destination_{suffix}"))

        cancel.set()

        # I leave last one working by intent (to finish consuming after all rebalances)
        for consumer_index in range(NUMBER_OF_CONCURRENT_CONSUMERS - 1):
            table_name = f"{table_name_prefix}_{suffix}_{consumer_index}"
            logging.debug(f"Dropping test.{table_name}_consumer{consumer_index}")
            instance.query(f"DROP TABLE IF EXISTS test.{table_name}_consumer{consumer_index} SYNC")

        def check_callback(res):
            logging.debug(f"Waiting for finishing consuming (have {res}, should be {msg_index[0]})")
            return int(res) >= msg_index[0]

        instance.query_with_retry(f"SELECT uniqExact(key) FROM test.kafka_destination_{suffix}",
                                  check_callback=check_callback)

        logging.debug(instance.query(f"SELECT count(), uniqExact(key), max(key) + 1 FROM test.kafka_destination_{suffix}"))
        result = int(instance.query(f"SELECT count() == uniqExact(key) FROM test.kafka_destination_{suffix}"))

        for consumer_index in range(NUMBER_OF_CONCURRENT_CONSUMERS):
            table_name = f"{table_name_prefix}_{suffix}_{consumer_index}"
            instance.query(f"""
                DROP TABLE IF EXISTS test.{table_name}_consumer{consumer_index};
                DROP TABLE IF EXISTS test.{table_name}_consumer{consumer_index}_mv;
            """)

        instance.query(f"DROP TABLE IF EXISTS test.kafka_destination_{suffix}")

        kafka_thread.join()

        assert result == 1, "Messages from kafka get duplicated!"
