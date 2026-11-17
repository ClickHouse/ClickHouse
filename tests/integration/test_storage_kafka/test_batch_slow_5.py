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


def test_formats_errors(kafka_cluster):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    for format_name in [
        "Template",
        "Regexp",
        "TSV",
        "TSVWithNamesAndTypes",
        "TSKV",
        "CSV",
        "CSVWithNames",
        "CSVWithNamesAndTypes",
        "CustomSeparated",
        "CustomSeparatedWithNames",
        "CustomSeparatedWithNamesAndTypes",
        "Values",
        "JSON",
        "JSONEachRow",
        "JSONStringsEachRow",
        "JSONCompactEachRow",
        "JSONCompactEachRowWithNamesAndTypes",
        "JSONObjectEachRow",
        "Avro",
        "RowBinary",
        "RowBinaryWithNamesAndTypes",
        "MsgPack",
        "JSONColumns",
        "JSONCompactColumns",
        "JSONColumnsWithMetadata",
        "BSONEachRow",
        "Native",
        "Arrow",
        "Parquet",
        "ORC",
        "JSONCompactColumns",
        "Npy",
        "ParquetMetadata",
        "CapnProto",
        "Protobuf",
        "ProtobufSingle",
        "ProtobufList",
        "DWARF",
        "HiveText",
        "MySQLDump",
    ]:
        with k.kafka_topic(admin_client, format_name):
            table_name = f"kafka_{format_name}"

            instance.query(
                f"""
                DROP TABLE IF EXISTS test.view;
                DROP TABLE IF EXISTS test.{table_name};

                CREATE TABLE test.{table_name} (key UInt64, value UInt64)
                    ENGINE = Kafka
                    SETTINGS kafka_broker_list = 'kafka1:19092',
                            kafka_topic_list = '{format_name}',
                            kafka_group_name = '{format_name}',
                            kafka_format = '{format_name}',
                            kafka_max_rows_per_message = 5,
                            format_template_row='template_row.format',
                            format_regexp='id: (.+?)',
                            input_format_with_names_use_header=0,
                            format_schema='key_value_message:Message';

                CREATE MATERIALIZED VIEW test.view ENGINE=MergeTree ORDER BY (key, value) AS
                    SELECT key, value FROM test.{table_name};
            """
            )

            k.kafka_produce(
                kafka_cluster,
                format_name,
                ["Broken message\nBroken message\nBroken message\n"],
            )

            num_errors = int(
                instance.query_with_retry(
                    f"SELECT length(exceptions.text) from system.kafka_consumers where database = 'test' and table = '{table_name}'",
                    check_callback=lambda res: int(res) > 0,
                )
            )

            assert num_errors > 0

            instance.query(f"DROP TABLE test.{table_name}")
            instance.query("DROP TABLE test.view")
