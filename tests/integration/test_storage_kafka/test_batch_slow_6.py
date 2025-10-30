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
    messages = [json.dumps({"key": j + 1, "value": "x" * 300}) for j in range(22)]
    k.kafka_produce(kafka_cluster, "handling_commit_failure", messages)

    instance.query(
        """
        DROP TABLE IF EXISTS test.view SYNC;
        DROP TABLE IF EXISTS test.consumer SYNC;

        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'handling_commit_failure',
                     kafka_group_name = 'handling_commit_failure',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 20,
                     kafka_flush_interval_ms = 1000;

        CREATE TABLE test.view (key UInt64, value String)
            ENGINE = MergeTree()
            ORDER BY key;
    """
    )

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka
            WHERE NOT sleepEachRow(0.25);
    """
    )

    instance.wait_for_log_line("Polled batch of 20 messages")
    # the tricky part here is that disconnect should happen after write prefix, but before we do commit
    # we have 0.25 (sleepEachRow) * 20 ( Rows ) = 5 sec window after "Polled batch of 20 messages"
    # while materialized view is working to inject zookeeper failure

    with kafka_cluster.pause_container("kafka1"):
        instance.wait_for_log_line("timeout", timeout=60, look_behind_lines=100)

    # kafka_cluster.open_bash_shell('instance')
    instance.wait_for_log_line("Committed offset 22")

    uniq_and_max = instance.query("SELECT uniqExact(key), max(key) FROM test.view")
    count = instance.query("SELECT count() FROM test.view")
    logging.debug(uniq_and_max)
    logging.debug(count)

    instance.query(
        """
        DROP TABLE test.consumer SYNC;
        DROP TABLE test.view SYNC;
    """
    )

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
        topic_name = format_name + k.get_topic_postfix(create_query_generator)
        table_name = f"kafka_{format_name}"
        logging.debug(f"Checking format {format_name}")
        with k.kafka_topic(admin_client, topic_name):
            create_query = create_query_generator(
                table_name,
                "key UInt64, value UInt64",
                topic_list=topic_name,
                consumer_group=topic_name,
                format=format_name,
            )

            instance.query(
                f"""
                DROP TABLE IF EXISTS test.view;
                DROP TABLE IF EXISTS test.{table_name};

                {create_query};

                CREATE MATERIALIZED VIEW test.view ENGINE=MergeTree ORDER BY (key, value) AS
                    SELECT key, value FROM test.{table_name};

                INSERT INTO test.{table_name} SELECT number * 10 as key, number * 100 as value FROM numbers({num_rows}) settings max_block_size=12, optimize_trivial_insert_select=0;
            """
            )
            messages = k.kafka_consume_with_retry(
                kafka_cluster, topic_name, message_count, need_decode=False
            )
            assert len(messages) == message_count

            rows = int(
                instance.query_with_retry(
                    "SELECT count() FROM test.view",
                    check_callback=lambda res: int(res) == num_rows,
                )
            )

            assert rows == num_rows

            result = instance.query("SELECT * FROM test.view ORDER by key")
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
    NUMBER_OF_CONSURRENT_CONSUMERS = 5

    instance.query(
        """
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination (
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
    """
    )

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
                for _ in range(59):
                    messages.append(
                        json.dumps({"key": msg_index[0], "value": msg_index[0]})
                    )
                    msg_index[0] += 1
                k.kafka_produce(kafka_cluster, topic_name, messages)

        kafka_thread = threading.Thread(target=produce)
        kafka_thread.start()

        for consumer_index in range(NUMBER_OF_CONSURRENT_CONSUMERS):
            table_name = f"{table_name_prefix}{consumer_index}"
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
                CREATE MATERIALIZED VIEW test.{table_name}_mv TO test.destination AS
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
            # kafka_cluster.open_bash_shell('instance')
            # Waiting for test.kafka_consumerX to start consume ...
            instance.wait_for_log_line(log_line.format(table_name))

        cancel.set()

        # I leave last one working by intent (to finish consuming after all rebalances)
        for consumer_index in range(NUMBER_OF_CONSURRENT_CONSUMERS - 1):
            logging.debug(("Dropping test.kafka_consumer{}".format(consumer_index)))
            instance.query(
                "DROP TABLE IF EXISTS test.kafka_consumer{} SYNC".format(consumer_index)
            )

        # logging.debug(instance.query('SELECT count(), uniqExact(key), max(key) + 1 FROM test.destination'))
        # kafka_cluster.open_bash_shell('instance')

        while 1:
            messages_consumed = int(
                instance.query("SELECT uniqExact(key) FROM test.destination")
            )
            if messages_consumed >= msg_index[0]:
                break
            time.sleep(1)
            logging.debug(
                (
                    "Waiting for finishing consuming (have {}, should be {})".format(
                        messages_consumed, msg_index[0]
                    )
                )
            )

        logging.debug(
            (
                instance.query(
                    "SELECT count(), uniqExact(key), max(key) + 1 FROM test.destination"
                )
            )
        )

        # Some queries to debug...
        # SELECT * FROM test.destination where key in (SELECT key FROM test.destination group by key having count() <> 1)
        # select number + 1 as key from numbers(4141) x left join test.destination using (key) where  test.destination.key = 0;
        # SELECT * FROM test.destination WHERE key between 2360 and 2370 order by key;
        # select _partition from test.destination group by _partition having count() <> max(_offset) + 1;
        # select toUInt64(0) as _partition, number + 1 as _offset from numbers(400) x left join test.destination using (_partition,_offset) where test.destination.key = 0 order by _offset;
        # SELECT * FROM test.destination WHERE _partition = 0 and _offset between 220 and 240 order by _offset;

        # CREATE TABLE test.reference (key UInt64, value UInt64) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka1:19092',
        #             kafka_topic_list = 'topic_with_multiple_partitions',
        #             kafka_group_name = 'rebalance_test_group_reference',
        #             kafka_format = 'JSONEachRow',
        #             kafka_max_block_size = 100000;
        #
        # CREATE MATERIALIZED VIEW test.reference_mv Engine=Log AS
        #     SELECT  key, value, _topic,_key,_offset, _partition, _timestamp, 'reference' as _consumed_by
        # FROM test.reference;
        #
        # select * from test.reference_mv left join test.destination using (key,_topic,_offset,_partition) where test.destination._consumed_by = '';

        result = int(
            instance.query("SELECT count() == uniqExact(key) FROM test.destination")
        )

        for consumer_index in range(NUMBER_OF_CONSURRENT_CONSUMERS):
            logging.debug(("kafka_consumer{}".format(consumer_index)))
            table_name = "kafka_consumer{}".format(consumer_index)
            instance.query(
                """
                DROP TABLE IF EXISTS test.{0};
                DROP TABLE IF EXISTS test.{0}_mv;
            """.format(
                    table_name
                )
            )

        instance.query(
            """
            DROP TABLE IF EXISTS test.destination;
        """
        )

        kafka_thread.join()

        assert result == 1, "Messages from kafka get duplicated!"
