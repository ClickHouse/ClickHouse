import time
import logging
import pytest

from helpers.cluster import ClickHouseCluster
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer, BrokerConnection
from kafka.admin import NewTopic

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml"],
    with_kafka=True,
)


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


def get_kafka_producer(port, serializer, retries):
    errors = []
    for _ in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers="localhost:{}".format(port),
                value_serializer=serializer,
            )
            logging.debug("Kafka Connection establised: localhost:{}".format(port))
            return producer
        except Exception as e:
            errors += [str(e)]
            time.sleep(1)

    raise Exception("Connection not establised, {}".format(errors))


def producer_serializer(x):
    return x.encode() if isinstance(x, str) else x


def kafka_produce(kafka_cluster, topic, messages, timestamp=None, retries=15):
    logging.debug(
        "kafka_produce server:{}:{} topic:{}".format(
            "localhost", kafka_cluster.kafka_port, topic
        )
    )
    producer = get_kafka_producer(
        kafka_cluster.kafka_port, producer_serializer, retries
    )
    for message in messages:
        producer.send(topic=topic, value=message, timestamp_ms=timestamp)
        producer.flush()


@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        kafka_id = instance.cluster.kafka_docker_id
        print(("kafka_id is {}".format(kafka_id)))
        yield cluster
    finally:
        cluster.shutdown()


def test_bad_messages_parsing(kafka_cluster):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    for format_name in [
        "TSV",
        "TSKV",
        "CSV",
        "Values",
        "JSON",
        "JSONEachRow",
        "JSONCompactEachRow",
        "JSONObjectEachRow",
        "Avro",
        "RowBinary",
        "JSONColumns",
        "JSONColumnsWithMetadata",
        "Native",
        "Arrow",
        "ArrowStream",
        "Parquet",
        "ORC",
        "JSONCompactColumns",
        "BSONEachRow",
        "MySQLDump",
    ]:
        print(format_name)

        kafka_create_topic(admin_client, f"{format_name}_err")

        instance.query(
            f"""
            DROP TABLE IF EXISTS view;
            DROP TABLE IF EXISTS kafka;

            CREATE TABLE kafka (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{format_name}_err',
                         kafka_group_name = '{format_name}',
                         kafka_format = '{format_name}',
                         kafka_handle_error_mode='stream';

            CREATE MATERIALIZED VIEW view Engine=Log AS
                SELECT _error FROM kafka WHERE length(_error) != 0 ;
        """
        )

        messages = ["qwertyuiop", "asdfghjkl", "zxcvbnm"]
        kafka_produce(kafka_cluster, f"{format_name}_err", messages)

        attempt = 0
        rows = 0
        while attempt < 500:
            rows = int(instance.query("SELECT count() FROM view"))
            if rows == len(messages):
                break
            attempt += 1

        assert rows == len(messages)

        kafka_delete_topic(admin_client, f"{format_name}_err")

    protobuf_schema = """
syntax = "proto3";

message Message {
  uint64 key = 1;
  uint64 value = 2;
};
"""

    instance.create_format_schema("schema_test_errors.proto", protobuf_schema)

    for format_name in ["Protobuf", "ProtobufSingle", "ProtobufList"]:
        instance.query(
            f"""
            DROP TABLE IF EXISTS view;
            DROP TABLE IF EXISTS kafka;

            CREATE TABLE kafka (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{format_name}_err',
                         kafka_group_name = '{format_name}',
                         kafka_format = '{format_name}',
                         kafka_handle_error_mode='stream',
                         kafka_schema='schema_test_errors:Message';

            CREATE MATERIALIZED VIEW view Engine=Log AS
                SELECT _error FROM kafka WHERE length(_error) != 0 ;
        """
        )

        print(format_name)

        kafka_create_topic(admin_client, f"{format_name}_err")

        messages = ["qwertyuiop", "poiuytrewq", "zxcvbnm"]
        kafka_produce(kafka_cluster, f"{format_name}_err", messages)

        attempt = 0
        rows = 0
        while attempt < 500:
            rows = int(instance.query("SELECT count() FROM view"))
            if rows == len(messages):
                break
            attempt += 1

        assert rows == len(messages)

        kafka_delete_topic(admin_client, f"{format_name}_err")

    capn_proto_schema = """
@0xd9dd7b35452d1c4f;

struct Message
{
    key @0 : UInt64;
    value @1 : UInt64;
}
"""

    instance.create_format_schema("schema_test_errors.capnp", capn_proto_schema)
    instance.query(
        f"""
            DROP TABLE IF EXISTS view;
            DROP TABLE IF EXISTS kafka;
    
            CREATE TABLE kafka (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = 'CapnProto_err',
                         kafka_group_name = 'CapnProto',
                         kafka_format = 'CapnProto',
                         kafka_handle_error_mode='stream',
                         kafka_schema='schema_test_errors:Message';
    
            CREATE MATERIALIZED VIEW view Engine=Log AS
                SELECT _error FROM kafka WHERE length(_error) != 0 ;                
        """
    )

    print("CapnProto")

    kafka_create_topic(admin_client, "CapnProto_err")

    messages = ["qwertyuiop", "asdfghjkl", "zxcvbnm"]
    kafka_produce(kafka_cluster, "CapnProto_err", messages)

    attempt = 0
    rows = 0
    while attempt < 500:
        rows = int(instance.query("SELECT count() FROM view"))
        if rows == len(messages):
            break
        attempt += 1

    assert rows == len(messages)

    kafka_delete_topic(admin_client, "CapnProto_err")


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
