import logging
import string
import time
from contextlib import contextmanager

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

@contextmanager
def existing_kafka_topic(admin_client, topic_name, max_retries=50):
    try:
        yield None
    finally:
        kafka_delete_topic(admin_client, topic_name, max_retries)

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

def get_admin_client(kafka_cluster):
    return KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )


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

def create_settings_string(settings):
    if settings is None:
        return ""

    def format_value(value):
        if isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, bool):
            return str(int(value))
        return str(value)

    settings_string = "SETTINGS "
    keys = settings.keys()
    first_key = next(iter(settings))
    settings_string += str(first_key) + " = " + format_value(settings[first_key])
    for key in keys:
        if key == first_key:
            continue
        settings_string += ", " + str(key) + " = " + format_value(settings[key])
    return settings_string

def generate_new_create_table_query(
    table_name,
    columns_def,
    database="test",
    brokers="{kafka_broker}:19092",
    topic_list="{kafka_topic_new}",
    consumer_group="{kafka_group_name_new}",
    format="{kafka_format_json_each_row}",
    row_delimiter="\\n",
    keeper_path=None,
    replica_name=None,
    settings=None,
):
    if settings is None:
        settings = {}
    if keeper_path is None:
        keeper_path = f"/clickhouse/{{database}}/{table_name}"
    if replica_name is None:
        replica_name = "r1"
    settings["kafka_keeper_path"] = keeper_path
    settings["kafka_replica_name"] = replica_name
    settings_string = create_settings_string(settings)
    query = f"""CREATE TABLE {database}.{table_name} ({columns_def}) ENGINE = Kafka('{brokers}', '{topic_list}', '{consumer_group}', '{format}', '{row_delimiter}')
{settings_string}
SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1"""
    logging.debug(f"Generated new create query: {query}")
    return query