import io
import logging
import os
import socket
import time
from contextlib import contextmanager

import avro.datafile
import avro.io
import avro.schema
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from google.protobuf.internal.encoder import _VarintBytes
from helpers.client import QueryRuntimeException
from helpers.test_tools import TSV
from kafka import BrokerConnection, KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.protocol.admin import DescribeGroupsRequest_v1
from kafka.protocol.group import MemberAssignment

# protoc --version
# libprotoc 3.0.0
# # to create kafka_pb2.py
# protoc --python_out=. kafka.proto
from test_storage_kafka import kafka_pb2, social_pb2


@contextmanager
def kafka_topic(
    admin_client,
    topic_name,
    num_partitions=1,
    replication_factor=1,
    max_retries=50,
    config=None,
):
    kafka_create_topic(
        admin_client,
        topic_name,
        num_partitions,
        replication_factor,
        max_retries,
        config,
    )
    try:
        yield None
    finally:
        # Code to release resource, e.g.:
        kafka_delete_topic(admin_client, topic_name, max_retries)


@contextmanager
def existing_kafka_topic(admin_client, topic_name, max_retries=50):
    try:
        yield None
    finally:
        kafka_delete_topic(admin_client, topic_name, max_retries)


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


def kafka_consume(kafka_cluster, topic, need_decode=True, timestamp=0):
    consumer = KafkaConsumer(
        bootstrap_servers=f"localhost:{kafka_cluster.kafka_port}",
        auto_offset_reset="earliest",
        session_timeout_ms=1000,
    )
    consumer.subscribe(topics=(topic))
    for toppar, messages in list(consumer.poll(5000).items()):
        if toppar.topic == topic:
            for message in messages:
                assert timestamp == 0 or message.timestamp / 1000 == timestamp
                if need_decode:
                    yield message.value.decode()
                else:
                    yield message.value
    consumer.unsubscribe()
    consumer.close()


def kafka_produce_protobuf_messages(kafka_cluster, topic, start_index, num_messages):
    data = b""
    for i in range(start_index, start_index + num_messages):
        msg = kafka_pb2.KeyValuePair()
        msg.key = i
        msg.value = str(i)
        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg
    producer = KafkaProducer(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port),
        value_serializer=producer_serializer,
    )
    producer.send(topic=topic, value=data)
    producer.flush()
    logging.debug(("Produced {} messages for topic {}".format(num_messages, topic)))


def kafka_consume_with_retry(
    kafka_cluster,
    topic,
    expected_messages,
    need_decode=True,
    timestamp=0,
    retry_count=20,
    sleep_time=0.1,
):
    messages = []
    try_count = 0
    while try_count < retry_count:
        try_count += 1
        messages.extend(
            kafka_consume(
                kafka_cluster, topic, need_decode=need_decode, timestamp=timestamp
            )
        )
        if len(messages) == expected_messages:
            break
        time.sleep(sleep_time)
    if len(messages) != expected_messages:
        raise Exception(f"Got only {len(messages)} messages")
    return messages


def kafka_produce_protobuf_messages_no_delimiters(
    kafka_cluster, topic, start_index, num_messages
):
    producer = KafkaProducer(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )
    for i in range(start_index, start_index + num_messages):
        msg = kafka_pb2.KeyValuePair()
        msg.key = i
        msg.value = str(i)
        serialized_msg = msg.SerializeToString()
        producer.send(topic=topic, value=serialized_msg)
    producer.flush()
    logging.debug("Produced {} messages for topic {}".format(num_messages, topic))


def kafka_produce_protobuf_social(kafka_cluster, topic, start_index, num_messages):
    data = b""
    for i in range(start_index, start_index + num_messages):
        msg = social_pb2.User()
        msg.username = "John Doe {}".format(i)
        msg.timestamp = 1000000 + i
        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg
    producer = KafkaProducer(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port),
        value_serializer=producer_serializer,
    )
    producer.send(topic=topic, value=data)
    producer.flush()
    logging.debug(("Produced {} messages for topic {}".format(num_messages, topic)))


def avro_message(value):
    schema = avro.schema.make_avsc_object(
        {
            "name": "row",
            "type": "record",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "blockNo", "type": "int"},
                {"name": "val1", "type": "string"},
                {"name": "val2", "type": "float"},
                {"name": "val3", "type": "int"},
            ],
        }
    )
    bytes_writer = io.BytesIO()

    # DataFileWrite seems to be mandatory to get schema encoded
    writer = avro.datafile.DataFileWriter(bytes_writer, avro.io.DatumWriter(), schema)
    if isinstance(value, list):
        for v in value:
            writer.append(v)
    else:
        writer.append(value)
    writer.flush()
    raw_bytes = bytes_writer.getvalue()

    writer.close()
    bytes_writer.close()
    return raw_bytes


def avro_confluent_message(schema_registry_client, value):
    serializer = MessageSerializer(schema_registry_client)
    schema = avro.schema.make_avsc_object(
        {
            "name": "row",
            "type": "record",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "blockNo", "type": "int"},
                {"name": "val1", "type": "string"},
                {"name": "val2", "type": "float"},
                {"name": "val3", "type": "int"},
            ],
        }
    )
    return serializer.encode_record_with_schema("test_subject", schema, value)


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


def generate_old_create_table_query(
    table_name,
    columns_def,
    database="test",
    brokers="{kafka_broker}:19092",
    topic_list="{kafka_topic_new}",
    consumer_group="{kafka_group_name_new}",
    format="{kafka_format_json_each_row}",
    row_delimiter="\\n",
    keeper_path=None,  # it is not used, but it is easier to handle keeper_path and replica_name like this
    replica_name=None,
    settings=None,
):
    settings_string = create_settings_string(settings)
    query = f"""CREATE TABLE {database}.{table_name} ({columns_def}) ENGINE = Kafka('{brokers}', '{topic_list}', '{consumer_group}', '{format}', '{row_delimiter}')
{settings_string}"""
    logging.debug(f"Generated old create query: {query}")
    return query


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


def must_use_thread_per_consumer(generator):
    if generator == generate_old_create_table_query:
        return False
    if generator == generate_new_create_table_query:
        return True
    raise Exception("Unexpected generator")


def get_topic_postfix(generator):
    if generator == generate_old_create_table_query:
        return "old"
    if generator == generate_new_create_table_query:
        return "new"
    raise Exception("Unexpected generator")


# Since everything is async and shaky when receiving messages from Kafka,
# we may want to try and check results multiple times in a loop.
def kafka_check_result(result, check=False, ref_file="test_kafka_json.reference"):
    fpath = os.path.join(os.path.dirname(__file__), ref_file)
    with open(fpath) as reference:
        if check:
            assert TSV(result) == TSV(reference)
        else:
            return TSV(result) == TSV(reference)


# https://stackoverflow.com/a/57692111/1555175
def describe_consumer_group(kafka_cluster, name):
    client = BrokerConnection("localhost", kafka_cluster.kafka_port, socket.AF_INET)
    client.connect_blocking()

    list_members_in_groups = DescribeGroupsRequest_v1(groups=[name])
    future = client.send(list_members_in_groups)
    while not future.is_done:
        for resp, f in client.recv():
            f.success(resp)

    (
        error_code,
        group_id,
        state,
        protocol_type,
        protocol,
        members,
    ) = future.value.groups[0]

    res = []
    for member in members:
        (member_id, client_id, client_host, member_metadata, member_assignment) = member
        member_info = {}
        member_info["member_id"] = member_id
        member_info["client_id"] = client_id
        member_info["client_host"] = client_host
        member_topics_assignment = []
        for topic, partitions in MemberAssignment.decode(member_assignment).assignment:
            member_topics_assignment.append({"topic": topic, "partitions": partitions})
        member_info["assignment"] = member_topics_assignment
        res.append(member_info)
    return res


def insert_with_retry(instance, values, table_name="kafka", max_try_count=5):
    try_count = 0
    while True:
        logging.debug(f"Inserting, try_count is {try_count}")
        try:
            try_count += 1
            instance.query(f"INSERT INTO test.{table_name} VALUES {values}")
            break
        except QueryRuntimeException as e:
            if "Local: Timed out." in str(e) and try_count < max_try_count:
                continue
            else:
                raise
