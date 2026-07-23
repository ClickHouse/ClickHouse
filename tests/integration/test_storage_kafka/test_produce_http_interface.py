import logging
import time

import pytest
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

from helpers.cluster import ClickHouseCluster, is_arm
from helpers.test_tools import TSV

if is_arm():
    pytestmark = pytest.mark.skip

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml", "configs/named_collection.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,  # For Replicated Table
    macros={
        "kafka_broker": "kafka1",
        "kafka_topic_old": "old",
        "kafka_group_name_old": "old",
        "kafka_topic_new": "new",
        "kafka_group_name_new": "new",
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
    instance.query("DROP DATABASE IF EXISTS test; CREATE DATABASE test;")
    # logging.debug("kafka is available - running test")
    yield  # run test


def kafka_create_topic(
    admin_client,
    topic_name,
    num_partitions=1,
    replication_factor=1,
    max_retries=50,
    config=None,
):
    logging.debug(
        f"Kafka create topic={topic_name}, num_partitions={num_partitions}, replication_factor={replication_factor}"
    )
    topics_list = [
        NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            topic_configs=config,
        )
    ]
    retries = 0
    while True:
        try:
            admin_client.create_topics(new_topics=topics_list, validate_only=False)
            logging.debug("Admin client succeed")
            return
        except Exception as e:
            retries += 1
            time.sleep(0.5)
            if retries < max_retries:
                logging.warning(f"Failed to create topic {e}")
            else:
                raise


def kafka_delete_topic(admin_client, topic, max_retries=50):
    result = admin_client.delete_topics([topic])
    for topic, e in result.topic_error_codes:
        if e == 0:
            logging.debug(f"Topic {topic} deleted")
        else:
            logging.error(f"Failed to delete topic {topic}: {e}")

    retries = 0
    while True:
        topics_listed = admin_client.list_topics()
        logging.debug(f"TOPICS LISTED: {topics_listed}")
        if topic not in topics_listed:
            return
        else:
            retries += 1
            time.sleep(0.5)
            if retries > max_retries:
                raise Exception(f"Failed to delete topics {topic}, {result}")


def test_kafka_produce_http_interface_row_based_format(kafka_cluster):
    # reproduction of #61060 with validating the written messages
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    topic_prefix = "http_row_"

    # It is important to have:
    # - long enough messages
    # - enough messages
    # I don't know the exact requirement for message sizes, but it doesn't reproduce with short messages
    # For the number of messages it seems like at least 3 messages is necessary
    expected_key = "01234567890123456789"
    expected_value = "aaaaabbbbbccccc"

    insert_query_end = f"(key, value) VALUES ('{expected_key}', '{expected_value}'), ('{expected_key}', '{expected_value}'), ('{expected_key}', '{expected_value}')"
    insert_query_template = "INSERT INTO {table_name} " + insert_query_end

    extra_settings = {
        "Protobuf": ", kafka_schema = 'string_key_value.proto:StringKeyValuePair'",
        "CapnProto": ", kafka_schema='string_key_value:StringKeyValuePair'",
        "Template": ", format_template_row='string_key_value.format'",
    }

    # Only the formats that can be used both and input and output format are tested
    # Reasons to exclude following formats:
    #  - JSONStrings: not actually an input format
    #  - ProtobufSingle: I cannot make it work to parse the messages. Probably something is broken,
    #    because the producer can write multiple rows into a same message, which makes them impossible to parse properly. Should added after #67549 is fixed.
    #  - ProtobufList: I didn't want to deal with the envelope and stuff
    #  - Npy: supports only single column
    #  - LineAsString: supports only single column
    #  - RawBLOB: supports only single column
    formats_to_test = [
        "TabSeparated",
        "TabSeparatedRaw",
        "TabSeparatedWithNames",
        "TabSeparatedWithNamesAndTypes",
        "TabSeparatedRawWithNames",
        "TabSeparatedRawWithNamesAndTypes",
        "Template",
        "CSV",
        "CSVWithNames",
        "CSVWithNamesAndTypes",
        "CustomSeparated",
        "CustomSeparatedWithNames",
        "CustomSeparatedWithNamesAndTypes",
        "Values",
        "JSON",
        "JSONColumns",
        "JSONColumnsWithMetadata",
        "JSONCompact",
        "JSONCompactColumns",
        "JSONEachRow",
        "JSONStringsEachRow",
        "JSONCompactEachRow",
        "JSONCompactEachRowWithNames",
        "JSONCompactEachRowWithNamesAndTypes",
        "JSONCompactStringsEachRow",
        "JSONCompactStringsEachRowWithNames",
        "JSONCompactStringsEachRowWithNamesAndTypes",
        "JSONObjectEachRow",
        "BSONEachRow",
        "TSKV",
        "Protobuf",
        "Avro",
        "Parquet",
        "Arrow",
        "ArrowStream",
        "ORC",
        "RowBinary",
        "RowBinaryWithNames",
        "RowBinaryWithNamesAndTypes",
        "Native",
        "CapnProto",
        "MsgPack",
    ]
    for format in formats_to_test:
        logging.debug(f"Creating tables and writing messages to {format}")
        topic = topic_prefix + format
        kafka_create_topic(admin_client, topic)

        extra_setting = extra_settings.get(format, "")

        # kafka_max_rows_per_message is set to 2 to make sure every format produces at least 2 messages, thus increasing the chance of catching a bug
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.view_{topic};
            DROP TABLE IF EXISTS test.consumer_{topic};
            CREATE TABLE test.kafka_writer_{topic} (key String, value String)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                        kafka_topic_list = '{topic}',
                        kafka_group_name = '{topic}',
                        kafka_format = '{format}',
                        kafka_max_rows_per_message = 2 {extra_setting};

            CREATE TABLE test.kafka_{topic} (key String, value String)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                        kafka_topic_list = '{topic}',
                        kafka_group_name = '{topic}',
                        kafka_format = '{format}' {extra_setting};

            CREATE MATERIALIZED VIEW test.view_{topic} Engine=Log AS
                SELECT key, value FROM test.kafka_{topic};
            """
        )
        instance.http_query(
            insert_query_template.format(table_name="test.kafka_writer_" + topic),
            method="POST",
        )

    expected = f"""\
{expected_key}\t{expected_value}
{expected_key}\t{expected_value}
{expected_key}\t{expected_value}
"""
    # give some times for the readers to read the messages
    for format in formats_to_test:
        logging.debug(f"Checking result for {format}")
        topic = topic_prefix + format

        result = instance.query_with_retry(
            f"SELECT * FROM test.view_{topic}",
            check_callback=lambda res: res.count("\n") == 3,
        )

        assert TSV(result) == TSV(expected)

        kafka_delete_topic(admin_client, topic)


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
