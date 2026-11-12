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
def test_row_based_formats(kafka_cluster, create_query_generator):
    admin_client = k.get_admin_client(kafka_cluster)

    for format_name in [
        "TSV",
        "TSVWithNamesAndTypes",
        "TSKV",
        "CSV",
        "CSVWithNamesAndTypes",
        "CustomSeparatedWithNamesAndTypes",
        "Values",
        "JSON",
        "JSONEachRow",
        "JSONCompactEachRow",
        "JSONCompactEachRowWithNamesAndTypes",
        "JSONObjectEachRow",
        "Avro",
        "RowBinary",
        "RowBinaryWithNamesAndTypes",
        "MsgPack",
    ]:
        logging.debug("Checking {format_name}")

        topic_name = format_name + k.get_topic_postfix(create_query_generator)
        table_name = f"kafka_{format_name}"

        with k.kafka_topic(admin_client, topic_name):
            num_rows = 10
            max_rows_per_message = 5
            message_count = num_rows / max_rows_per_message

            create_query = create_query_generator(
                table_name,
                "key UInt64, value UInt64",
                topic_list=topic_name,
                consumer_group=topic_name,
                format=format_name,
                settings={"kafka_max_rows_per_message": max_rows_per_message},
            )

            instance.query(
                f"""
                DROP TABLE IF EXISTS test.view;
                DROP TABLE IF EXISTS test.{table_name};

                {create_query};

                CREATE MATERIALIZED VIEW test.view ENGINE=MergeTree ORDER BY (key, value) AS
                    SELECT key, value FROM test.{table_name};

                INSERT INTO test.{table_name} SELECT number * 10 as key, number * 100 as value FROM numbers({num_rows});
            """
            )

            messages = k.kafka_consume_with_retry(
                kafka_cluster, topic_name, message_count, need_decode=False
            )

            assert len(messages) == message_count

            instance.query_with_retry(
                "SELECT count() FROM test.view",
                check_callback=lambda res: int(res) == num_rows,
            )

            result = instance.query("SELECT * FROM test.view")
            expected = ""
            for i in range(num_rows):
                expected += str(i * 10) + "\t" + str(i * 100) + "\n"
            assert result == expected
