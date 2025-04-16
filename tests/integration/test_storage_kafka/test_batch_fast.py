"""Quick tests, faster than 30 seconds"""

import json
import logging
import math
import random
import threading
import time

import avro.datafile
import avro.io
import avro.schema
import kafka.errors
import pytest
from confluent_kafka.avro.cached_schema_registry_client import (
    CachedSchemaRegistryClient,
)
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from google.protobuf.internal.encoder import _VarintBytes
from kafka import BrokerConnection, KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.protocol.admin import DescribeGroupsRequest_v1
from kafka.protocol.group import MemberAssignment

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, is_arm
from helpers.network import PartitionManager
from helpers.test_tools import TSV, assert_eq_with_retry

from . import common as k
from . import message_with_repeated_pb2

# protoc --version
# libprotoc 3.0.0
# # to create kafka_pb2.py
# protoc --python_out=. kafka.proto


if is_arm():
    pytestmark = pytest.mark.skip

# TODO: add test for run-time offset update in CH, if we manually update it on Kafka side.
# TODO: add test for SELECT LIMIT is working.


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
    "create_query_generator, do_direct_read",
    [
        (k.generate_old_create_table_query, True),
        (k.generate_new_create_table_query, False),
    ],
)
def test_kafka_column_types(kafka_cluster, create_query_generator, do_direct_read):
    def assert_returned_exception(e):
        assert e.value.returncode == 36
        assert (
            "KafkaEngine doesn't support DEFAULT/MATERIALIZED/EPHEMERAL expressions for columns."
            in str(e.value)
        )

    # check column with DEFAULT expression
    with pytest.raises(QueryRuntimeException) as exception:
        instance.query(create_query_generator("kafka", "a Int, b Int DEFAULT 0"))
    assert_returned_exception(exception)

    # check EPHEMERAL
    with pytest.raises(QueryRuntimeException) as exception:
        instance.query(create_query_generator("kafka", "a Int, b Int EPHEMERAL"))
    assert_returned_exception(exception)

    # check MATERIALIZED
    with pytest.raises(QueryRuntimeException) as exception:
        instance.query(
            """
                CREATE TABLE test.kafka (a Int, b String MATERIALIZED toString(a))
                ENGINE = Kafka('{kafka_broker}:19092', '{kafka_topic_new}', '{kafka_group_name_new}', '{kafka_format_json_each_row}', '\\n')
                """
        )
    assert_returned_exception(exception)

    if do_direct_read:
        # check ALIAS
        instance.query(
            create_query_generator(
                "kafka",
                "a Int, b String Alias toString(a)",
                settings={"kafka_commit_on_select": True},
            )
        )
        messages = []
        for i in range(5):
            messages.append(json.dumps({"a": i}))
        k.kafka_produce(kafka_cluster, k.KAFKA_TOPIC_NEW, messages)
        result = ""
        expected = TSV(
            """
    0\t0
    1\t1
    2\t2
    3\t3
    4\t4
                                """
        )
        retries = 50
        while retries > 0:
            result += instance.query("SELECT a, b FROM test.kafka", ignore_error=True)
            if TSV(result) == expected:
                break
            retries -= 1
            time.sleep(0.5)

        assert TSV(result) == expected

        instance.query("DROP TABLE test.kafka SYNC")


def test_kafka_settings_old_syntax(kafka_cluster):
    assert TSV(
        instance.query(
            "SELECT * FROM system.macros WHERE macro like 'kafka%' ORDER BY macro",
            ignore_error=True,
        )
    ) == TSV(
        f"""kafka_broker	kafka1
kafka_client_id	instance
kafka_format_json_each_row	JSONEachRow
kafka_group_name_new	{k.KAFKA_CONSUMER_GROUP_NEW}
kafka_group_name_old	{k.KAFKA_CONSUMER_GROUP_OLD}
kafka_topic_new	new_t
kafka_topic_old	old_t
"""
    )

    instance.query(
        """
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka('{kafka_broker}:19092', '{kafka_topic_old}', '{kafka_group_name_old}', '{kafka_format_json_each_row}', '\\n')
            SETTINGS kafka_commit_on_select = 1;
        """
    )

    # Don't insert malformed messages since old settings syntax
    # doesn't support skipping of broken messages.
    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    k.kafka_produce(kafka_cluster, k.KAFKA_TOPIC_OLD, messages)

    result = ""
    while True:
        result += instance.query("SELECT * FROM test.kafka", ignore_error=True)
        if k.kafka_check_result(result):
            break

    k.kafka_check_result(result, True)

    members = k.describe_consumer_group(kafka_cluster, k.KAFKA_CONSUMER_GROUP_OLD)
    assert members[0]["client_id"] == "ClickHouse-instance-test-kafka"
    # text_desc = kafka_cluster.exec_in_container(kafka_cluster.get_container_id('kafka1'),"kafka-consumer-groups --bootstrap-server localhost:9092 --describe --members --group old --verbose"))


def test_kafka_settings_new_syntax(kafka_cluster):
    instance.query(
        """
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = '{kafka_broker}:19092',
                     kafka_topic_list = '{kafka_topic_new}',
                     kafka_group_name = '{kafka_group_name_new}',
                     kafka_format = '{kafka_format_json_each_row}',
                     kafka_row_delimiter = '\\n',
                     kafka_commit_on_select = 1,
                     kafka_client_id = '{kafka_client_id} test 1234',
                     kafka_skip_broken_messages = 1;
        """
    )

    messages = []
    for i in range(25):
        messages.append(json.dumps({"key": i, "value": i}))
    k.kafka_produce(kafka_cluster, k.KAFKA_TOPIC_NEW, messages)

    # Insert couple of malformed messages.
    k.kafka_produce(kafka_cluster, k.KAFKA_TOPIC_NEW, ["}{very_broken_message,"])
    k.kafka_produce(kafka_cluster, k.KAFKA_TOPIC_NEW, ["}another{very_broken_message,"])

    messages = []
    for i in range(25, 50):
        messages.append(json.dumps({"key": i, "value": i}))
    k.kafka_produce(kafka_cluster, k.KAFKA_TOPIC_NEW, messages)

    result = ""
    while True:
        result += instance.query("SELECT * FROM test.kafka", ignore_error=True)
        if k.kafka_check_result(result):
            break

    k.kafka_check_result(result, True)

    members = k.describe_consumer_group(kafka_cluster, k.KAFKA_CONSUMER_GROUP_NEW)
    assert members[0]["client_id"] == "instance test 1234"


def test_kafka_settings_predefined_macros(kafka_cluster):
    instance.query(
        """
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = '{kafka_broker}:19092',
                     kafka_topic_list = '{database}_{table}_topic',
                     kafka_group_name = '{database}_{table}_group',
                     kafka_format = '{kafka_format_json_each_row}',
                     kafka_row_delimiter = '\\n',
                     kafka_commit_on_select = 1,
                     kafka_client_id = '{database}_{table} test 1234',
                     kafka_skip_broken_messages = 1;
        """
    )

    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    k.kafka_produce(kafka_cluster, "test_kafka_topic", messages)

    result = instance.query("SELECT * FROM test.kafka", ignore_error=True)
    k.kafka_check_result(result, True)

    members = k.describe_consumer_group(kafka_cluster, "test_kafka_group")
    assert members[0]["client_id"] == "test_kafka test 1234"


def test_kafka_json_as_string(kafka_cluster):
    k.kafka_produce(
        kafka_cluster,
        "kafka_json_as_string",
        [
            '{"t": 123, "e": {"x": "woof"} }',
            "",
            '{"t": 124, "e": {"x": "test"} }',
            '{"F1":"V1","F2":{"F21":"V21","F22":{},"F23":"V23","F24":"2019-12-24T16:28:04"},"F3":"V3"}',
        ],
    )

    # 'tombstone' record (null value) = marker of deleted record
    producer = KafkaProducer(
        bootstrap_servers="localhost:{}".format(cluster.kafka_port),
        value_serializer=k.producer_serializer,
        key_serializer=k.producer_serializer,
    )
    producer.send(topic="kafka_json_as_string", key="xxx")
    producer.flush()

    instance.query(
        """
        CREATE TABLE test.kafka (field String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'kafka_json_as_string',
                     kafka_group_name = 'kafka_json_as_string',
                     kafka_commit_on_select = 1,
                     kafka_format = 'JSONAsString',
                     kafka_flush_interval_ms=1000;
        """
    )

    result = instance.query("SELECT * FROM test.kafka;")
    expected = """\
{"t": 123, "e": {"x": "woof"} }
{"t": 124, "e": {"x": "test"} }
{"F1":"V1","F2":{"F21":"V21","F22":{},"F23":"V23","F24":"2019-12-24T16:28:04"},"F3":"V3"}
"""
    assert TSV(result) == TSV(expected)
    assert instance.contains_in_log(
        "Parsing of message (topic: kafka_json_as_string, partition: 0, offset: [0-9]*) return no rows"
    )


def test_kafka_issue11308(kafka_cluster):
    # Check that matview does respect Kafka SETTINGS
    k.kafka_produce(
        kafka_cluster,
        "issue11308",
        [
            '{"t": 123, "e": {"x": "woof"} }',
            '{"t": 123, "e": {"x": "woof"} }',
            '{"t": 124, "e": {"x": "test"} }',
        ],
    )

    instance.query(
        """
        CREATE TABLE test.persistent_kafka (
            time UInt64,
            some_string String
        )
        ENGINE = MergeTree()
        ORDER BY time;

        CREATE TABLE test.kafka (t UInt64, `e.x` String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'issue11308',
                     kafka_group_name = 'issue11308',
                     kafka_format = 'JSONEachRow',
                     kafka_row_delimiter = '\\n',
                     kafka_flush_interval_ms=1000,
                     input_format_import_nested_json = 1;

        CREATE MATERIALIZED VIEW test.persistent_kafka_mv TO test.persistent_kafka AS
        SELECT
            `t` AS `time`,
            `e.x` AS `some_string`
        FROM test.kafka;
        """
    )

    while int(instance.query("SELECT count() FROM test.persistent_kafka")) < 3:
        time.sleep(1)

    result = instance.query("SELECT * FROM test.persistent_kafka ORDER BY time;")

    instance.query(
        """
        DROP TABLE test.persistent_kafka;
        DROP TABLE test.persistent_kafka_mv;
    """
    )

    expected = """\
123	woof
123	woof
124	test
"""
    assert TSV(result) == TSV(expected)


def test_kafka_issue4116(kafka_cluster):
    # Check that format_csv_delimiter parameter works now - as part of all available format settings.
    k.kafka_produce(
        kafka_cluster,
        "issue4116",
        ["1|foo", "2|bar", "42|answer", "100|multi\n101|row\n103|message"],
    )

    instance.query(
        """
        CREATE TABLE test.kafka (a UInt64, b String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'issue4116',
                     kafka_group_name = 'issue4116',
                     kafka_commit_on_select = 1,
                     kafka_format = 'CSV',
                     kafka_row_delimiter = '\\n',
                     format_csv_delimiter = '|';
        """
    )

    result = instance.query("SELECT * FROM test.kafka ORDER BY a;")

    expected = """\
1	foo
2	bar
42	answer
100	multi
101	row
103	message
"""
    assert TSV(result) == TSV(expected)


def test_kafka_consumer_hang(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)

    topic_name = "consumer_hang"
    k.kafka_create_topic(admin_client, topic_name, num_partitions=8)

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic_name}',
                     kafka_group_name = '{topic_name}',
                     kafka_format = 'JSONEachRow',
                     kafka_num_consumers = 8;
        CREATE TABLE test.view (key UInt64, value UInt64) ENGINE = Memory();
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS SELECT * FROM test.kafka;
        """
    )

    instance.wait_for_log_line("kafka.*Stalled", repetitions=20)

    # This should trigger heartbeat fail,
    # which will trigger REBALANCE_IN_PROGRESS,
    # and which can lead to consumer hang.
    with kafka_cluster.pause_container("kafka1"):
        instance.wait_for_log_line("heartbeat error")

    # logging.debug("Attempt to drop")
    instance.query("DROP TABLE test.kafka")

    # kafka_cluster.open_bash_shell('instance')

    instance.query(
        """
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    """
    )

    # original problem appearance was a sequence of the following messages in librdkafka logs:
    # BROKERFAIL -> |ASSIGN| -> REBALANCE_IN_PROGRESS -> "waiting for rebalance_cb" (repeated forever)
    # so it was waiting forever while the application will execute queued rebalance callback

    # from a user perspective: we expect no hanging 'drop' queries
    # 'dr'||'op' to avoid self matching
    assert (
        int(
            instance.query(
                "select count() from system.processes where position(lower(query),'dr'||'op')>0"
            )
        )
        == 0
    )

    # cleanup unread messages so kafka will not wait reading consumers to delete topic
    instance.query(
        f"""
            CREATE TABLE test.kafka (key UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic_name}',
                     kafka_commit_on_select = 1,
                     kafka_group_name = '{topic_name}',
                     kafka_format = 'JSONEachRow',
                     kafka_num_consumers = 8;
        """
    )

    num_read = int(instance.query("SELECT count() FROM test.kafka"))
    logging.debug(f"read {num_read} from {topic_name} before delete")
    instance.query("DROP TABLE test.kafka")
    k.kafka_delete_topic(admin_client, topic_name)


def test_kafka_consumer_hang2(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)

    topic_name = "consumer_hang2"
    k.kafka_create_topic(admin_client, topic_name)

    instance.query(
        """
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.kafka2;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'consumer_hang2',
                     kafka_group_name = 'consumer_hang2',
                     kafka_commit_on_select = 1,
                     kafka_format = 'JSONEachRow';

        CREATE TABLE test.kafka2 (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'consumer_hang2',
                     kafka_commit_on_select = 1,
                     kafka_group_name = 'consumer_hang2',
                     kafka_format = 'JSONEachRow';
        """
    )

    # first consumer subscribe the topic, try to poll some data, and go to rest
    instance.query("SELECT * FROM test.kafka")

    # second consumer do the same leading to rebalance in the first
    # consumer, try to poll some data
    instance.query("SELECT * FROM test.kafka2")

    # echo 'SELECT * FROM test.kafka; SELECT * FROM test.kafka2; DROP TABLE test.kafka;' | clickhouse client -mn &
    #    kafka_cluster.open_bash_shell('instance')

    # first consumer has pending rebalance callback unprocessed (no poll after select)
    # one of those queries was failing because of
    # https://github.com/edenhill/librdkafka/issues/2077
    # https://github.com/edenhill/librdkafka/issues/2898
    instance.query("DROP TABLE test.kafka")
    instance.query("DROP TABLE test.kafka2")

    # from a user perspective: we expect no hanging 'drop' queries
    # 'dr'||'op' to avoid self matching
    assert (
        int(
            instance.query(
                "select count() from system.processes where position(lower(query),'dr'||'op')>0"
            )
        )
        == 0
    )
    k.kafka_delete_topic(admin_client, topic_name)


# sequential read from different consumers leads to breaking lot of kafka invariants
# (first consumer will get all partitions initially, and may have problems in doing polls every 60 sec)
def test_kafka_read_consumers_in_parallel(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)

    topic_name = "read_consumers_in_parallel"
    k.kafka_create_topic(admin_client, topic_name, num_partitions=8)

    cancel = threading.Event()

    def produce():
        while not cancel.is_set():
            messages = []
            for _ in range(100):
                messages.append(json.dumps({"key": 0, "value": 0}))
            k.kafka_produce(kafka_cluster, "read_consumers_in_parallel", messages)
            time.sleep(1)

    kafka_thread = threading.Thread(target=produce)
    kafka_thread.start()

    # when we have more than 1 consumer in a single table,
    # and kafka_thread_per_consumer=0
    # all the consumers should be read in parallel, not in sequence.
    # then reading in parallel 8 consumers with 1 seconds kafka_poll_timeout_ms and less than 1 sec limit
    # we should have exactly 1 poll per consumer (i.e. 8 polls) every 1 seconds (from different threads)
    # in case parallel consuming is not working we will have only 1 poll every 1 seconds (from the same thread).
    instance.query(
        f"""
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic_name}',
                     kafka_group_name = '{topic_name}',
                     kafka_format = 'JSONEachRow',
                     kafka_num_consumers = 8,
                     kafka_thread_per_consumer = 0,
                     kafka_poll_timeout_ms = 1000,
                     kafka_flush_interval_ms = 999;
        CREATE TABLE test.view (key UInt64, value UInt64) ENGINE = Memory();
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS SELECT * FROM test.kafka;
        """
    )

    instance.wait_for_log_line(
        "kafka.*Polled batch of [0-9]+.*read_consumers_in_parallel",
        repetitions=64,
        look_behind_lines=100,
        timeout=30,  # we should get 64 polls in ~8 seconds, but when read sequentially it will take more than 64 sec
    )

    cancel.set()
    kafka_thread.join()

    instance.query(
        """
        DROP TABLE test.consumer;
        DROP TABLE test.view;
        DROP TABLE test.kafka;
    """
    )
    k.kafka_delete_topic(admin_client, topic_name)


def test_kafka_csv_with_delimiter(kafka_cluster):
    messages = []
    for i in range(50):
        messages.append("{i}, {i}".format(i=i))
    k.kafka_produce(kafka_cluster, "csv", messages)

    instance.query(
        """
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'csv',
                     kafka_commit_on_select = 1,
                     kafka_group_name = 'csv',
                     kafka_format = 'CSV';
        """
    )

    result = ""
    while True:
        result += instance.query("SELECT * FROM test.kafka", ignore_error=True)
        if k.kafka_check_result(result):
            break

    k.kafka_check_result(result, True)


def test_kafka_tsv_with_delimiter(kafka_cluster):
    messages = []
    for i in range(50):
        messages.append("{i}\t{i}".format(i=i))
    k.kafka_produce(kafka_cluster, "tsv", messages)

    instance.query(
        """
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'tsv',
                     kafka_commit_on_select = 1,
                     kafka_group_name = 'tsv',
                     kafka_format = 'TSV';
        """
    )

    result = ""
    while True:
        result += instance.query("SELECT * FROM test.kafka", ignore_error=True)
        if k.kafka_check_result(result):
            break

    k.kafka_check_result(result, True)


def test_kafka_select_empty(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)
    topic_name = "empty"
    k.kafka_create_topic(admin_client, topic_name)

    instance.query(
        f"""
        CREATE TABLE test.kafka (key UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic_name}',
                     kafka_commit_on_select = 1,
                     kafka_group_name = '{topic_name}',
                     kafka_format = 'TSV',
                     kafka_row_delimiter = '\\n';
        """
    )

    assert int(instance.query("SELECT count() FROM test.kafka")) == 0
    k.kafka_delete_topic(admin_client, topic_name)


def test_kafka_json_without_delimiter(kafka_cluster):
    messages = ""
    for i in range(25):
        messages += json.dumps({"key": i, "value": i}) + "\n"
    k.kafka_produce(kafka_cluster, "json", [messages])

    messages = ""
    for i in range(25, 50):
        messages += json.dumps({"key": i, "value": i}) + "\n"
    k.kafka_produce(kafka_cluster, "json", [messages])

    instance.query(
        """
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'json',
                     kafka_group_name = 'json',
                     kafka_commit_on_select = 1,
                     kafka_format = 'JSONEachRow';
        """
    )

    result = ""
    while True:
        result += instance.query("SELECT * FROM test.kafka", ignore_error=True)
        if k.kafka_check_result(result):
            break

    k.kafka_check_result(result, True)


def test_kafka_protobuf(kafka_cluster):
    k.kafka_produce_protobuf_messages(kafka_cluster, "pb", 0, 20)
    k.kafka_produce_protobuf_messages(kafka_cluster, "pb", 20, 1)
    k.kafka_produce_protobuf_messages(kafka_cluster, "pb", 21, 29)

    instance.query(
        """
        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'pb',
                     kafka_group_name = 'pb',
                     kafka_format = 'Protobuf',
                     kafka_commit_on_select = 1,
                     kafka_schema = 'kafka.proto:KeyValuePair';
        """
    )

    result = ""
    while True:
        result += instance.query("SELECT * FROM test.kafka", ignore_error=True)
        if k.kafka_check_result(result):
            break

    k.kafka_check_result(result, True)


def test_kafka_string_field_on_first_position_in_protobuf(kafka_cluster):
    # https://github.com/ClickHouse/ClickHouse/issues/12615
    k.kafka_produce_protobuf_social(
        kafka_cluster, "string_field_on_first_position_in_protobuf", 0, 20
    )
    k.kafka_produce_protobuf_social(
        kafka_cluster, "string_field_on_first_position_in_protobuf", 20, 1
    )
    k.kafka_produce_protobuf_social(
        kafka_cluster, "string_field_on_first_position_in_protobuf", 21, 29
    )

    instance.query(
        """
CREATE TABLE test.kafka (
    username String,
    timestamp Int32
  ) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092',
    kafka_topic_list = 'string_field_on_first_position_in_protobuf',
    kafka_group_name = 'string_field_on_first_position_in_protobuf',
    kafka_format = 'Protobuf',
    kafka_commit_on_select = 1,
    kafka_schema = 'social:User';
        """
    )

    result = instance.query("SELECT * FROM test.kafka", ignore_error=True)
    expected = """\
John Doe 0	1000000
John Doe 1	1000001
John Doe 2	1000002
John Doe 3	1000003
John Doe 4	1000004
John Doe 5	1000005
John Doe 6	1000006
John Doe 7	1000007
John Doe 8	1000008
John Doe 9	1000009
John Doe 10	1000010
John Doe 11	1000011
John Doe 12	1000012
John Doe 13	1000013
John Doe 14	1000014
John Doe 15	1000015
John Doe 16	1000016
John Doe 17	1000017
John Doe 18	1000018
John Doe 19	1000019
John Doe 20	1000020
John Doe 21	1000021
John Doe 22	1000022
John Doe 23	1000023
John Doe 24	1000024
John Doe 25	1000025
John Doe 26	1000026
John Doe 27	1000027
John Doe 28	1000028
John Doe 29	1000029
John Doe 30	1000030
John Doe 31	1000031
John Doe 32	1000032
John Doe 33	1000033
John Doe 34	1000034
John Doe 35	1000035
John Doe 36	1000036
John Doe 37	1000037
John Doe 38	1000038
John Doe 39	1000039
John Doe 40	1000040
John Doe 41	1000041
John Doe 42	1000042
John Doe 43	1000043
John Doe 44	1000044
John Doe 45	1000045
John Doe 46	1000046
John Doe 47	1000047
John Doe 48	1000048
John Doe 49	1000049
"""
    assert TSV(result) == TSV(expected)


def test_kafka_protobuf_no_delimiter(kafka_cluster):
    instance.query(
        """
        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'pb_no_delimiter',
                     kafka_group_name = 'pb_no_delimiter',
                     kafka_format = 'ProtobufSingle',
                     kafka_commit_on_select = 1,
                     kafka_schema = 'kafka.proto:KeyValuePair';
        """
    )

    k.kafka_produce_protobuf_messages_no_delimiters(
        kafka_cluster, "pb_no_delimiter", 0, 20
    )
    k.kafka_produce_protobuf_messages_no_delimiters(
        kafka_cluster, "pb_no_delimiter", 20, 1
    )
    k.kafka_produce_protobuf_messages_no_delimiters(
        kafka_cluster, "pb_no_delimiter", 21, 29
    )

    result = ""
    while True:
        result += instance.query("SELECT * FROM test.kafka", ignore_error=True)
        if k.kafka_check_result(result):
            break

    k.kafka_check_result(result, True)

    instance.query(
        """
    CREATE TABLE test.kafka_writer (key UInt64, value String)
        ENGINE = Kafka
        SETTINGS kafka_broker_list = 'kafka1:19092',
                    kafka_topic_list = 'pb_no_delimiter',
                    kafka_group_name = 'pb_no_delimiter',
                    kafka_format = 'ProtobufSingle',
                    kafka_commit_on_select = 1,
                    kafka_schema = 'kafka.proto:KeyValuePair';
    """
    )

    instance.query(
        "INSERT INTO test.kafka_writer VALUES (13,'Friday'),(42,'Answer to the Ultimate Question of Life, the Universe, and Everything'), (110, 'just a number')"
    )

    time.sleep(1)

    result = instance.query("SELECT * FROM test.kafka ORDER BY key", ignore_error=True)

    expected = """\
13	Friday
42	Answer to the Ultimate Question of Life, the Universe, and Everything
110	just a number
"""
    assert TSV(result) == TSV(expected)


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_materialized_view(kafka_cluster, create_query_generator):
    topic_name = "mv"

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        DROP TABLE IF EXISTS test.kafka;

        {create_query_generator("kafka", "key UInt64, value UInt64", topic_list=topic_name, consumer_group="mv")};
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    """
    )

    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    k.kafka_produce(kafka_cluster, topic_name, messages)

    with k.existing_kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        result = instance.query_with_retry(
            "SELECT * FROM test.view", check_callback=k.kafka_check_result
        )

        k.kafka_check_result(result, True)

        instance.query(
            """
            DROP TABLE test.consumer;
            DROP TABLE test.view;
            DROP TABLE test.kafka;
        """
        )


@pytest.mark.parametrize(
    "create_query_generator, log_line",
    [
        (
            k.generate_new_create_table_query,
            r"kafka.*Saved offset [0-9]+ for topic-partition \[recreate_kafka_table:[0-9]+",
        ),
        (
            k.generate_old_create_table_query,
            "kafka.*Committed offset [0-9]+.*recreate_kafka_table",
        ),
    ],
)
def test_kafka_recreate_kafka_table(kafka_cluster, create_query_generator, log_line):
    """
    Checks that materialized view work properly after dropping and recreating the Kafka table.
    """
    topic_name = "recreate_kafka_table"
    thread_per_consumer = k.must_use_thread_per_consumer(create_query_generator)

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name, num_partitions=6):
        create_query = create_query_generator(
            "kafka",
            "key UInt64, value UInt64",
            topic_list=topic_name,
            consumer_group="recreate_kafka_table_group",
            settings={
                "kafka_num_consumers": 4,
                "kafka_flush_interval_ms": 1000,
                "kafka_skip_broken_messages": 1048577,
                "kafka_thread_per_consumer": thread_per_consumer,
            },
        )

        instance.query(
            f"""
            DROP TABLE IF EXISTS test.view;
            DROP TABLE IF EXISTS test.consumer;
            {create_query};

            CREATE TABLE test.view (key UInt64, value UInt64)
                ENGINE = MergeTree()
                ORDER BY key;
            CREATE MATERIALIZED VIEW test.consumer TO test.view AS
                SELECT * FROM test.kafka;
        """
        )

        messages = []
        for i in range(120):
            messages.append(json.dumps({"key": i, "value": i}))
        k.kafka_produce(kafka_cluster, "recreate_kafka_table", messages)

        instance.wait_for_log_line(
            log_line,
            repetitions=6,
            look_behind_lines=100,
        )

        instance.query(
            """
            DROP TABLE test.kafka;
        """
        )

        instance.rotate_logs()

        k.kafka_produce(kafka_cluster, "recreate_kafka_table", messages)

        instance.query(create_query)

        instance.wait_for_log_line(
            log_line,
            repetitions=6,
            look_behind_lines=100,
        )

        # data was not flushed yet (it will be flushed 7.5 sec after creating MV)
        assert (
            int(
                instance.query_with_retry(
                    sql="SELECT count() FROM test.view",
                    check_callback=lambda x: x == 240,
                )
            )
            == 240
        )

        instance.query(
            """
            DROP TABLE test.consumer;
            DROP TABLE test.kafka;
            DROP TABLE test.view;
        """
        )


@pytest.mark.parametrize(
    "create_query_generator, log_line",
    [
        (k.generate_old_create_table_query, "Committed offset {offset}"),
        (
            k.generate_new_create_table_query,
            r"kafka.*Saved offset [0-9]+ for topic-partition \[{topic}:[0-9]+\]",
        ),
    ],
)
def test_librdkafka_compression(kafka_cluster, create_query_generator, log_line):
    """
    Regression for UB in snappy-c (that is used in librdkafka),
    backport pr is [1].

      [1]: https://github.com/ClickHouse-Extras/librdkafka/pull/3

    Example of corruption:

        2020.12.10 09:59:56.831507 [ 20 ] {} <Error> void DB::StorageKafka::threadFunc(size_t): Code: 27. DB::Exception: Cannot parse input: expected '"' before: 'foo"}': (while reading the value of key value): (at row 1)

    To trigger this regression there should duplicated messages

    Orignal reproducer is:
    $ gcc --version |& fgrep gcc
    gcc (GCC) 10.2.0
    $ yes foobarbaz | fold -w 80 | head -n10 >| in-…
    $ make clean && make CFLAGS='-Wall -g -O2 -ftree-loop-vectorize -DNDEBUG=1 -DSG=1 -fPIC'
    $ ./verify in
    final comparision of in failed at 20 of 100

    """

    supported_compression_types = ["gzip", "snappy", "lz4", "zstd", "uncompressed"]

    messages = []
    expected = []

    value = "foobarbaz" * 10
    number_of_messages = 50
    for i in range(number_of_messages):
        messages.append(json.dumps({"key": i, "value": value}))
        expected.append(f"{i}\t{value}")

    expected = "\n".join(expected)

    admin_client = k.get_admin_client(kafka_cluster)

    for compression_type in supported_compression_types:
        logging.debug(("Check compression {}".format(compression_type)))

        topic_name = "test_librdkafka_compression_{}".format(compression_type)
        topic_config = {"compression.type": compression_type}
        with k.kafka_topic(admin_client, topic_name, config=topic_config):
            instance.query(
                """{create_query};

                CREATE TABLE test.view (key UInt64, value String)
                    ENGINE = MergeTree()
                    ORDER BY key;

                CREATE MATERIALIZED VIEW test.consumer TO test.view AS
                    SELECT * FROM test.kafka;
            """.format(
                    create_query=create_query_generator(
                        "kafka",
                        "key UInt64, value String",
                        topic_list=topic_name,
                        format="JSONEachRow",
                        settings={"kafka_flush_interval_ms": 1000},
                    ),
                )
            )

            k.kafka_produce(kafka_cluster, topic_name, messages)

            instance.wait_for_log_line(
                log_line.format(offset=number_of_messages, topic=topic_name)
            )
            result = instance.query("SELECT * FROM test.view")
            assert TSV(result) == TSV(expected)

            instance.query("DROP TABLE test.kafka SYNC")
            instance.query("DROP TABLE test.consumer SYNC")
            instance.query("DROP TABLE test.view SYNC")


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_new_create_table_query, k.generate_old_create_table_query],
)
def test_kafka_materialized_view_with_subquery(kafka_cluster, create_query_generator):
    topic_name = "mysq"
    logging.debug(f"Using topic {topic_name}")

    create_query = create_query_generator(
        "kafka",
        "key UInt64, value UInt64",
        topic_list=topic_name,
        consumer_group=topic_name,
    )
    instance.query(
        f"""
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;

        {create_query};
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM (SELECT * FROM test.kafka);
    """
    )

    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    k.kafka_produce(kafka_cluster, topic_name, messages)

    with k.existing_kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        result = instance.query_with_retry(
            "SELECT * FROM test.view",
            check_callback=k.kafka_check_result,
            retry_count=40,
            sleep_time=0.75,
        )

        instance.query(
            """
            DROP TABLE test.consumer;
            DROP TABLE test.view;
        """
        )

        k.kafka_check_result(result, True)


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_many_materialized_views(kafka_cluster, create_query_generator):
    topic_name = f"mmv-{k.get_topic_postfix(create_query_generator)}"
    create_query = create_query_generator(
        "kafka",
        "key UInt64, value UInt64",
        topic_list=topic_name,
        consumer_group=f"{topic_name}-group",
    )

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.view1;
        DROP TABLE IF EXISTS test.view2;
        DROP TABLE IF EXISTS test.consumer1;
        DROP TABLE IF EXISTS test.consumer2;
        {create_query};
        CREATE TABLE test.view1 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE TABLE test.view2 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer1 TO test.view1 AS
            SELECT * FROM test.kafka;
        CREATE MATERIALIZED VIEW test.consumer2 TO test.view2 AS
            SELECT * FROM test.kafka;
    """
    )

    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    k.kafka_produce(kafka_cluster, topic_name, messages)

    with k.existing_kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        result1 = instance.query_with_retry(
            "SELECT * FROM test.view1", check_callback=k.kafka_check_result
        )
        result2 = instance.query_with_retry(
            "SELECT * FROM test.view2", check_callback=k.kafka_check_result
        )

        instance.query(
            """
            DROP TABLE test.consumer1;
            DROP TABLE test.consumer2;
            DROP TABLE test.view1;
            DROP TABLE test.view2;
        """
        )

        k.kafka_check_result(result1, True)
        k.kafka_check_result(result2, True)


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_flush_on_big_message(kafka_cluster, create_query_generator):
    # Create batches of messages of size ~100Kb
    kafka_messages = 1000
    batch_messages = 1000
    topic_name = "flush" + k.get_topic_postfix(create_query_generator)
    messages = [
        json.dumps({"key": i, "value": "x" * 100}) * batch_messages
        for i in range(kafka_messages)
    ]
    k.kafka_produce(kafka_cluster, topic_name, messages)

    admin_client = k.get_admin_client(kafka_cluster)

    with k.existing_kafka_topic(admin_client, topic_name):
        create_query = create_query_generator(
            "kafka",
            "key UInt64, value String",
            topic_list=topic_name,
            consumer_group=topic_name,
            settings={"kafka_max_block_size": 10},
        )

        instance.query(
            f"""
            DROP TABLE IF EXISTS test.view;
            DROP TABLE IF EXISTS test.consumer;
            {create_query};
            CREATE TABLE test.view (key UInt64, value String)
                ENGINE = MergeTree
                ORDER BY key;
            CREATE MATERIALIZED VIEW test.consumer TO test.view AS
                SELECT * FROM test.kafka;
        """
        )

        received = False
        while not received:
            try:
                offsets = admin_client.list_consumer_group_offsets(topic_name)
                for topic, offset in list(offsets.items()):
                    if topic.topic == topic_name and offset.offset == kafka_messages:
                        received = True
                        break
            except kafka.errors.GroupCoordinatorNotAvailableError:
                continue

        while True:
            result = instance.query("SELECT count() FROM test.view")
            if int(result) == kafka_messages * batch_messages:
                break

        instance.query(
            """
            DROP TABLE test.consumer;
            DROP TABLE test.view;
        """
        )

        assert (
            int(result) == kafka_messages * batch_messages
        ), "ClickHouse lost some messages: {}".format(result)


def test_kafka_virtual_columns(kafka_cluster):
    topic_config = {
        # default retention, since predefined timestamp_ms is used.
        "retention.ms": "-1",
    }
    with k.kafka_topic(k.get_admin_client(kafka_cluster), "virt1", config=topic_config):
        instance.query(
            """
            CREATE TABLE test.kafka (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                        kafka_topic_list = 'virt1',
                        kafka_group_name = 'virt1',
                        kafka_commit_on_select = 1,
                        kafka_format = 'JSONEachRow';
            """
        )

        messages = ""
        for i in range(25):
            messages += json.dumps({"key": i, "value": i}) + "\n"
        k.kafka_produce(kafka_cluster, "virt1", [messages], 0)

        messages = ""
        for i in range(25, 50):
            messages += json.dumps({"key": i, "value": i}) + "\n"
        k.kafka_produce(kafka_cluster, "virt1", [messages], 0)

        result = ""
        while True:
            result += instance.query(
                """SELECT _key, key, _topic, value, _offset, _partition, _timestamp = 0 ? '0000-00-00 00:00:00' : toString(_timestamp) AS _timestamp FROM test.kafka""",
                ignore_error=True,
            )
            if k.kafka_check_result(result, False, "test_kafka_virtual1.reference"):
                break

        k.kafka_check_result(result, True, "test_kafka_virtual1.reference")


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_virtual_columns_with_materialized_view(
    kafka_cluster, create_query_generator
):
    topic_config = {
        # default retention, since predefined timestamp_ms is used.
        "retention.ms": "-1",
    }
    # the topic name is hardcoded in reference, it doesn't worth to create two reference files to have separate topics,
    # as the context manager will always clean up the topic
    topic_name = "virt2"
    create_query = create_query_generator(
        "kafka",
        "key UInt64, value UInt64",
        topic_list=topic_name,
        consumer_group=f"{topic_name}-group",
    )
    with k.kafka_topic(
        k.get_admin_client(kafka_cluster), topic_name, config=topic_config
    ):
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.view;
            DROP TABLE IF EXISTS test.consumer;
            {create_query};
            CREATE TABLE test.view (key UInt64, value UInt64, kafka_key String, topic String, offset UInt64, partition UInt64, timestamp Nullable(DateTime('UTC')))
                ENGINE = MergeTree()
                ORDER BY key;
            CREATE MATERIALIZED VIEW test.consumer TO test.view AS
                SELECT *, _key as kafka_key, _topic as topic, _offset as offset, _partition as partition, _timestamp = 0 ? '0000-00-00 00:00:00' : toString(_timestamp) as timestamp FROM test.kafka;
        """
        )

        messages = []
        for i in range(50):
            messages.append(json.dumps({"key": i, "value": i}))
        k.kafka_produce(kafka_cluster, topic_name, messages, 0)

        def check_callback(result):
            return k.kafka_check_result(result, False, "test_kafka_virtual2.reference")

        result = instance.query_with_retry(
            "SELECT kafka_key, key, topic, value, offset, partition, timestamp FROM test.view ORDER BY kafka_key, key",
            check_callback=check_callback,
        )

        k.kafka_check_result(result, True, "test_kafka_virtual2.reference")

        instance.query(
            """
            DROP TABLE test.consumer;
            DROP TABLE test.view;
        """
        )


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_insert(kafka_cluster, create_query_generator):
    topic_name = "insert1" + k.get_topic_postfix(create_query_generator)

    instance.query(
        create_query_generator(
            "kafka",
            "key UInt64, value UInt64",
            topic_list=topic_name,
            consumer_group=topic_name,
            format="TSV",
        )
    )

    message_count = 50
    values = []
    for i in range(message_count):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    with k.existing_kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        k.insert_with_retry(instance, values)

        messages = k.kafka_consume_with_retry(kafka_cluster, topic_name, message_count)
        result = "\n".join(messages)
        k.kafka_check_result(result, True)


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_produce_consume(kafka_cluster, create_query_generator):
    topic_name = "insert2" + k.get_topic_postfix(create_query_generator)

    create_query = create_query_generator(
        "kafka",
        "key UInt64, value UInt64",
        topic_list=topic_name,
        consumer_group=topic_name,
        format="TSV",
    )
    instance.query(
        f"""
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        {create_query};
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    """
    )

    messages_num = 10000

    def insert():
        values = []
        for i in range(messages_num):
            values.append("({i}, {i})".format(i=i))
        values = ",".join(values)

        k.insert_with_retry(instance, values)

    threads = []
    threads_num = 16
    for _ in range(threads_num):
        threads.append(threading.Thread(target=insert))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    with k.existing_kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        expected_row_count = messages_num * threads_num
        result = instance.query_with_retry(
            "SELECT count() FROM test.view",
            sleep_time=1,
            retry_count=20,
            check_callback=lambda result: int(result) == expected_row_count,
        )

        instance.query(
            """
            DROP TABLE test.consumer;
            DROP TABLE test.view;
        """
        )

        for thread in threads:
            thread.join()

        assert (
            int(result) == expected_row_count
        ), "ClickHouse lost some messages: {}".format(result)


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_commit_on_block_write(kafka_cluster, create_query_generator):
    topic_name = "block" + k.get_topic_postfix(create_query_generator)
    create_query = create_query_generator(
        "kafka",
        "key UInt64, value UInt64",
        topic_list=topic_name,
        consumer_group=topic_name,
        settings={"kafka_max_block_size": 100},
    )
    instance.query(
        f"""
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        {create_query};
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    """
    )

    cancel = threading.Event()

    # We need to pass i as a reference. Simple integers are passed by value.
    # Making an array is probably the easiest way to "force pass by reference".
    i = [0]

    def produce(i):
        while not cancel.is_set():
            messages = []
            for _ in range(101):
                messages.append(json.dumps({"key": i[0], "value": i[0]}))
                i[0] += 1
            k.kafka_produce(kafka_cluster, topic_name, messages)

    kafka_thread = threading.Thread(target=produce, args=[i])
    kafka_thread.start()

    instance.query_with_retry(
        "SELECT count() FROM test.view",
        sleep_time=1,
        check_callback=lambda res: int(res) >= 100,
    )

    cancel.set()

    instance.query("DROP TABLE test.kafka SYNC")

    instance.query(create_query)
    kafka_thread.join()

    instance.query_with_retry(
        "SELECT uniqExact(key) FROM test.view",
        sleep_time=1,
        check_callback=lambda res: int(res) >= i[0],
    )

    result = int(instance.query("SELECT count() == uniqExact(key) FROM test.view"))

    instance.query(
        """
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    """
    )

    kafka_thread.join()

    assert result == 1, "Messages from kafka get duplicated!"


@pytest.mark.parametrize(
    "create_query_generator, log_line",
    [
        (k.generate_old_create_table_query, "kafka.*Committed offset 2.*virt2_[01]"),
        (
            k.generate_new_create_table_query,
            r"kafka.*Saved offset 2 for topic-partition \[virt2_[01]:[0-9]+",
        ),
    ],
)
def test_kafka_virtual_columns2(kafka_cluster, create_query_generator, log_line):
    admin_client = k.get_admin_client(kafka_cluster)

    topic_config = {
        # default retention, since predefined timestamp_ms is used.
        "retention.ms": "-1",
    }
    thread_per_consumer = k.must_use_thread_per_consumer(create_query_generator)
    topic_name_0 = "virt2_0"
    topic_name_1 = "virt2_1"
    consumer_group = "virt2" + k.get_topic_postfix(create_query_generator)
    with k.kafka_topic(
        admin_client, topic_name_0, num_partitions=2, config=topic_config
    ):
        with k.kafka_topic(
            admin_client, topic_name_1, num_partitions=2, config=topic_config
        ):
            create_query = create_query_generator(
                "kafka",
                "value UInt64",
                topic_list=f"{topic_name_0},{topic_name_1}",
                consumer_group=consumer_group,
                settings={
                    "kafka_num_consumers": 2,
                    "kafka_thread_per_consumer": thread_per_consumer,
                },
            )

            instance.query(
                f"""
                {create_query};

                CREATE MATERIALIZED VIEW test.view ENGINE=MergeTree ORDER BY tuple() AS
                SELECT value, _key, _topic, _partition, _offset, toUnixTimestamp(_timestamp), toUnixTimestamp64Milli(_timestamp_ms), _headers.name, _headers.value FROM test.kafka;
                """
            )

            producer = KafkaProducer(
                bootstrap_servers="localhost:{}".format(cluster.kafka_port),
                value_serializer=k.producer_serializer,
                key_serializer=k.producer_serializer,
            )

            producer.send(
                topic=topic_name_0,
                value=json.dumps({"value": 1}),
                partition=0,
                key="k1",
                timestamp_ms=1577836801001,
                headers=[("content-encoding", b"base64")],
            )
            producer.send(
                topic=topic_name_0,
                value=json.dumps({"value": 2}),
                partition=0,
                key="k2",
                timestamp_ms=1577836802002,
                headers=[
                    ("empty_value", b""),
                    ("", b"empty name"),
                    ("", b""),
                    ("repetition", b"1"),
                    ("repetition", b"2"),
                ],
            )
            producer.flush()

            producer.send(
                topic=topic_name_0,
                value=json.dumps({"value": 3}),
                partition=1,
                key="k3",
                timestamp_ms=1577836803003,
                headers=[("b", b"b"), ("a", b"a")],
            )
            producer.send(
                topic=topic_name_0,
                value=json.dumps({"value": 4}),
                partition=1,
                key="k4",
                timestamp_ms=1577836804004,
                headers=[("a", b"a"), ("b", b"b")],
            )
            producer.flush()

            producer.send(
                topic=topic_name_1,
                value=json.dumps({"value": 5}),
                partition=0,
                key="k5",
                timestamp_ms=1577836805005,
            )
            producer.send(
                topic=topic_name_1,
                value=json.dumps({"value": 6}),
                partition=0,
                key="k6",
                timestamp_ms=1577836806006,
            )
            producer.flush()

            producer.send(
                topic=topic_name_1,
                value=json.dumps({"value": 7}),
                partition=1,
                key="k7",
                timestamp_ms=1577836807007,
            )
            producer.send(
                topic=topic_name_1,
                value=json.dumps({"value": 8}),
                partition=1,
                key="k8",
                timestamp_ms=1577836808008,
            )
            producer.flush()

            instance.wait_for_log_line(log_line, repetitions=4, look_behind_lines=6000)

            members = k.describe_consumer_group(kafka_cluster, consumer_group)
            # pprint.pprint(members)
            # members[0]['client_id'] = 'ClickHouse-instance-test-kafka-0'
            # members[1]['client_id'] = 'ClickHouse-instance-test-kafka-1'

            result = instance.query(
                "SELECT * FROM test.view ORDER BY value", ignore_error=True
            )

            expected = f"""\
        1	k1	{topic_name_0}	0	0	1577836801	1577836801001	['content-encoding']	['base64']
        2	k2	{topic_name_0}	0	1	1577836802	1577836802002	['empty_value','','','repetition','repetition']	['','empty name','','1','2']
        3	k3	{topic_name_0}	1	0	1577836803	1577836803003	['b','a']	['b','a']
        4	k4	{topic_name_0}	1	1	1577836804	1577836804004	['a','b']	['a','b']
        5	k5	{topic_name_1}	0	0	1577836805	1577836805005	[]	[]
        6	k6	{topic_name_1}	0	1	1577836806	1577836806006	[]	[]
        7	k7	{topic_name_1}	1	0	1577836807	1577836807007	[]	[]
        8	k8	{topic_name_1}	1	1	1577836808	1577836808008	[]	[]
        """

            assert TSV(result) == TSV(expected)

            instance.query(
                """
                DROP TABLE test.kafka;
                DROP TABLE test.view;
            """
            )
            instance.rotate_logs()


@pytest.mark.parametrize(
    "create_query_generator, do_direct_read",
    [
        (k.generate_old_create_table_query, True),
        (k.generate_new_create_table_query, False),
    ],
)
def test_kafka_producer_consumer_separate_settings(
    kafka_cluster, create_query_generator, do_direct_read
):
    instance.rotate_logs()
    instance.query(
        create_query_generator(
            "test_kafka",
            "key UInt64",
            topic_list="separate_settings",
            consumer_group="test",
        )
    )

    if do_direct_read:
        instance.query("SELECT * FROM test.test_kafka")
    instance.query("INSERT INTO test.test_kafka VALUES (1)")

    assert instance.contains_in_log("Kafka producer created")
    assert instance.contains_in_log("Created #0 consumer")

    kafka_conf_warnings = instance.grep_in_log("rdk:CONFWARN")

    assert kafka_conf_warnings is not None

    for warn in kafka_conf_warnings.strip().split("\n"):
        # this setting was applied via old syntax and applied on both consumer
        # and producer configurations
        assert "heartbeat.interval.ms" in warn

    kafka_consumer_applied_properties = instance.grep_in_log("Consumer set property")
    kafka_producer_applied_properties = instance.grep_in_log("Producer set property")

    assert kafka_consumer_applied_properties is not None
    assert kafka_producer_applied_properties is not None

    # global settings should be applied for consumer and producer
    global_settings = {
        "debug": "topic,protocol,cgrp,consumer",
        "statistics.interval.ms": "600",
    }

    for name, value in global_settings.items():
        property_in_log = f"{name}:{value}"
        assert property_in_log in kafka_consumer_applied_properties
        assert property_in_log in kafka_producer_applied_properties

    settings_topic__separate_settings__consumer = {"session.timeout.ms": "6001"}

    for name, value in settings_topic__separate_settings__consumer.items():
        property_in_log = f"{name}:{value}"
        assert property_in_log in kafka_consumer_applied_properties
        assert property_in_log not in kafka_producer_applied_properties

    producer_settings = {"transaction.timeout.ms": "60001"}

    for name, value in producer_settings.items():
        property_in_log = f"{name}:{value}"
        assert property_in_log not in kafka_consumer_applied_properties
        assert property_in_log in kafka_producer_applied_properties

    # Should be ignored, because it is inside producer tag
    producer_legacy_syntax__topic_separate_settings = {"message.timeout.ms": "300001"}

    for name, value in producer_legacy_syntax__topic_separate_settings.items():
        property_in_log = f"{name}:{value}"
        assert property_in_log not in kafka_consumer_applied_properties
        assert property_in_log not in kafka_producer_applied_properties

    # Old syntax, applied on consumer and producer
    legacy_syntax__topic_separated_settings = {"heartbeat.interval.ms": "302"}

    for name, value in legacy_syntax__topic_separated_settings.items():
        property_in_log = f"{name}:{value}"
        assert property_in_log in kafka_consumer_applied_properties
        assert property_in_log in kafka_producer_applied_properties


@pytest.mark.parametrize(
    "create_query_generator, log_line",
    [
        (k.generate_new_create_table_query, "Saved offset 5"),
        (k.generate_old_create_table_query, "Committed offset 5"),
    ],
)
def test_kafka_produce_key_timestamp(kafka_cluster, create_query_generator, log_line):
    topic_name = "insert3"
    topic_config = {
        # default retention, since predefined timestamp_ms is used.
        "retention.ms": "-1",
    }

    with k.kafka_topic(
        k.get_admin_client(kafka_cluster), topic_name, config=topic_config
    ):
        writer_create_query = create_query_generator(
            "kafka_writer",
            "key UInt64, value UInt64, _key String, _timestamp DateTime('UTC')",
            topic_list=topic_name,
            consumer_group=topic_name,
            format="TSV",
        )
        reader_create_query = create_query_generator(
            "kafka",
            "key UInt64, value UInt64, inserted_key String, inserted_timestamp DateTime('UTC')",
            topic_list=topic_name,
            consumer_group=topic_name,
            format="TSV",
        )

        instance.query(
            f"""
            DROP TABLE IF EXISTS test.view;
            DROP TABLE IF EXISTS test.consumer;
            {writer_create_query};
            {reader_create_query};
            CREATE MATERIALIZED VIEW test.view ENGINE=MergeTree ORDER BY tuple() AS
                SELECT key, value, inserted_key, toUnixTimestamp(inserted_timestamp), _key, _topic, _partition, _offset, toUnixTimestamp(_timestamp) FROM test.kafka;
        """
        )

        instance.query(
            "INSERT INTO test.kafka_writer VALUES ({},{},'{}',toDateTime({}))".format(
                1, 1, "k1", 1577836801
            )
        )
        instance.query(
            "INSERT INTO test.kafka_writer VALUES ({},{},'{}',toDateTime({}))".format(
                2, 2, "k2", 1577836802
            )
        )
        instance.query(
            "INSERT INTO test.kafka_writer VALUES ({},{},'{}',toDateTime({})),({},{},'{}',toDateTime({}))".format(
                3, 3, "k3", 1577836803, 4, 4, "k4", 1577836804
            )
        )
        instance.query(
            "INSERT INTO test.kafka_writer VALUES ({},{},'{}',toDateTime({}))".format(
                5, 5, "k5", 1577836805
            )
        )

        instance.wait_for_log_line(log_line)

        expected = """\
    1	1	k1	1577836801	k1	insert3	0	0	1577836801
    2	2	k2	1577836802	k2	insert3	0	1	1577836802
    3	3	k3	1577836803	k3	insert3	0	2	1577836803
    4	4	k4	1577836804	k4	insert3	0	3	1577836804
    5	5	k5	1577836805	k5	insert3	0	4	1577836805
    """

        result = instance.query_with_retry(
            "SELECT * FROM test.view ORDER BY value",
            ignore_error=True,
            retry_count=5,
            sleep_time=1,
            check_callback=lambda res: TSV(res) == TSV(expected),
        )

        assert TSV(result) == TSV(expected)


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_insert_avro(kafka_cluster, create_query_generator):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )
    topic_config = {
        # default retention, since predefined timestamp_ms is used.
        "retention.ms": "-1",
    }
    topic_name = "avro1" + k.get_topic_postfix(create_query_generator)
    with k.kafka_topic(admin_client, topic_name, config=topic_config):
        create_query = create_query_generator(
            "kafka",
            "key UInt64, value UInt64, _timestamp DateTime('UTC')",
            topic_list=topic_name,
            consumer_group=topic_name,
            format="Avro",
        )
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.kafka;
            {create_query}
        """
        )

        instance.query(
            "INSERT INTO test.kafka select number*10 as key, number*100 as value, 1636505534 as _timestamp from numbers(4) SETTINGS output_format_avro_rows_in_file = 2, output_format_avro_codec = 'deflate'"
        )

        message_count = 2
        messages = k.kafka_consume_with_retry(
            kafka_cluster,
            topic_name,
            message_count,
            need_decode=False,
            timestamp=1636505534,
        )

        result = ""
        for a_message in messages:
            result += k.decode_avro(a_message) + "\n"

        expected_result = """{'key': 0, 'value': 0, '_timestamp': 1636505534}
{'key': 10, 'value': 100, '_timestamp': 1636505534}

{'key': 20, 'value': 200, '_timestamp': 1636505534}
{'key': 30, 'value': 300, '_timestamp': 1636505534}

"""
        assert result == expected_result


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_produce_consume_avro(kafka_cluster, create_query_generator):
    topic_name = "insert_avro" + k.get_topic_postfix(create_query_generator)
    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        num_rows = 75

        writer_create_query = create_query_generator(
            "kafka_writer",
            "key UInt64, value UInt64",
            topic_list=topic_name,
            consumer_group=topic_name,
            format="Avro",
        )

        reader_create_query = create_query_generator(
            "kafka",
            "key UInt64, value UInt64",
            topic_list=topic_name,
            consumer_group=topic_name,
            format="Avro",
        )

        instance.query(
            f"""
            DROP TABLE IF EXISTS test.view;
            DROP TABLE IF EXISTS test.kafka;
            DROP TABLE IF EXISTS test.kafka_writer;

            {writer_create_query};
            {reader_create_query};

            CREATE MATERIALIZED VIEW test.view ENGINE=MergeTree ORDER BY tuple() AS
                SELECT key, value FROM test.kafka;
            """
        )

        instance.query(
            "INSERT INTO test.kafka_writer select number*10 as key, number*100 as value from numbers({num_rows}) SETTINGS output_format_avro_rows_in_file = 7".format(
                num_rows=num_rows
            )
        )

        instance.wait_for_log_line(
            "Committed offset {offset}".format(offset=math.ceil(num_rows / 7))
        )

        expected_num_rows = instance.query(
            "SELECT COUNT(1) FROM test.view", ignore_error=True
        )
        assert int(expected_num_rows) == num_rows

        expected_max_key = instance.query(
            "SELECT max(key) FROM test.view", ignore_error=True
        )
        assert int(expected_max_key) == (num_rows - 1) * 10


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_flush_by_time(kafka_cluster, create_query_generator):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )
    topic_name = "flush_by_time" + k.get_topic_postfix(create_query_generator)

    with k.kafka_topic(admin_client, topic_name):
        create_query = create_query_generator(
            "kafka",
            "key UInt64, value UInt64",
            topic_list=topic_name,
            consumer_group=topic_name,
            settings={
                "kafka_max_block_size": 100,
            },
        )
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.view;
            DROP TABLE IF EXISTS test.consumer;

            {create_query};

            CREATE TABLE test.view (key UInt64, value UInt64, ts DateTime64(3) MATERIALIZED now64(3))
                ENGINE = MergeTree()
                ORDER BY key;
        """
        )

        cancel = threading.Event()

        def produce():
            while not cancel.is_set():
                messages = [json.dumps({"key": 0, "value": 0})]
                k.kafka_produce(kafka_cluster, topic_name, messages)
                time.sleep(0.8)

        kafka_thread = threading.Thread(target=produce)
        kafka_thread.start()

        instance.query(
            """
            CREATE MATERIALIZED VIEW test.consumer TO test.view AS
                SELECT * FROM test.kafka;
        """
        )

        # By default the flush timeout should be 7.5 seconds => 18 seconds should be enough for 2 flushes, but not for 3
        time.sleep(18)

        result = instance.query("SELECT uniqExact(ts), count() >= 15 FROM test.view")

        cancel.set()
        kafka_thread.join()

        instance.query(
            """
            DROP TABLE test.consumer;
            DROP TABLE test.view;
        """
        )

        assert TSV(result) == TSV("2	1")


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_flush_by_block_size(kafka_cluster, create_query_generator):
    topic_name = "flush_by_block_size" + k.get_topic_postfix(create_query_generator)

    cancel = threading.Event()

    def produce():
        while not cancel.is_set():
            messages = []
            messages.append(json.dumps({"key": 0, "value": 0}))
            k.kafka_produce(kafka_cluster, topic_name, messages)

    kafka_thread = threading.Thread(target=produce)

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        kafka_thread.start()

        create_query = create_query_generator(
            "kafka",
            "key UInt64, value UInt64",
            topic_list=topic_name,
            consumer_group=topic_name,
            settings={
                "kafka_max_block_size": 100,
                "kafka_poll_max_batch_size": 1,
                "kafka_flush_interval_ms": 120000,
            },
        )

        instance.query(
            f"""
            DROP TABLE IF EXISTS test.view;
            DROP TABLE IF EXISTS test.consumer;

            {create_query};

            CREATE TABLE test.view (key UInt64, value UInt64)
                ENGINE = MergeTree()
                ORDER BY key;

            CREATE MATERIALIZED VIEW test.consumer TO test.view AS
                SELECT * FROM test.kafka;
        """
        )

        # Wait for Kafka engine to consume this data
        while 1 != int(
            instance.query(
                "SELECT count() FROM system.parts WHERE database = 'test' AND table = 'view' AND name = 'all_1_1_0'"
            )
        ):
            time.sleep(0.5)

        cancel.set()
        kafka_thread.join()

        # more flushes can happens during test, we need to check only result of first flush (part named all_1_1_0).
        result = instance.query("SELECT count() FROM test.view WHERE _part='all_1_1_0'")
        # logging.debug(result)

        instance.query(
            """
            DROP TABLE test.consumer;
            DROP TABLE test.view;
        """
        )

        # 100 = first poll should return 100 messages (and rows)
        # not waiting for stream_flush_interval_ms
        assert (
            int(result) == 100
        ), "Messages from kafka should be flushed when block of size kafka_max_block_size is formed!"


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_lot_of_partitions_partial_commit_of_bulk(
    kafka_cluster, create_query_generator
):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    topic_name = "topic_with_multiple_partitions2" + k.get_topic_postfix(
        create_query_generator
    )
    with k.kafka_topic(admin_client, topic_name):
        create_query = create_query_generator(
            "kafka",
            "key UInt64, value UInt64",
            topic_list=topic_name,
            consumer_group=topic_name,
            settings={
                "kafka_max_block_size": 211,
                "kafka_flush_interval_ms": 500,
            },
        )
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.view;
            DROP TABLE IF EXISTS test.consumer;
            {create_query};
            CREATE TABLE test.view (key UInt64, value UInt64)
                ENGINE = MergeTree()
                ORDER BY key;
            CREATE MATERIALIZED VIEW test.consumer TO test.view AS
                SELECT * FROM test.kafka;
        """
        )

        messages = []
        count = 0
        for dummy_msg in range(1000):
            rows = []
            for dummy_row in range(random.randrange(3, 10)):
                count = count + 1
                rows.append(json.dumps({"key": count, "value": count}))
            messages.append("\n".join(rows))
        k.kafka_produce(kafka_cluster, topic_name, messages)

        instance.wait_for_log_line("kafka.*Stalled", repetitions=5)

        result = instance.query(
            "SELECT count(), uniqExact(key), max(key) FROM test.view"
        )
        logging.debug(result)
        assert TSV(result) == TSV("{0}\t{0}\t{0}".format(count))

        instance.query(
            """
            DROP TABLE test.consumer;
            DROP TABLE test.view;
        """
        )


# TODO(antaljanosbenjamin): find another way to make insertion fail
@pytest.mark.parametrize(
    "create_query_generator",
    [
        k.generate_old_create_table_query,
        # k.generate_new_create_table_query,
    ],
)
def test_kafka_no_holes_when_write_suffix_failed(kafka_cluster, create_query_generator):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )
    topic_name = "no_holes_when_write_suffix_failed" + k.get_topic_postfix(
        create_query_generator
    )

    with k.kafka_topic(admin_client, topic_name):
        messages = [json.dumps({"key": j + 1, "value": "x" * 300}) for j in range(22)]
        k.kafka_produce(kafka_cluster, topic_name, messages)

        create_query = create_query_generator(
            "kafka",
            "key UInt64, value String",
            topic_list=topic_name,
            consumer_group=topic_name,
            settings={
                "kafka_max_block_size": 20,
                "kafka_flush_interval_ms": 2000,
            },
        )
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.view SYNC;
            DROP TABLE IF EXISTS test.consumer;

            {create_query};

            CREATE TABLE test.view (key UInt64, value String)
                ENGINE = ReplicatedMergeTree('/clickhouse/kafkatest/tables/{topic_name}', 'node1')
                ORDER BY key;
        """
        )

        # init PartitionManager (it starts container) earlier
        pm = PartitionManager()

        instance.query(
            """
            CREATE MATERIALIZED VIEW test.consumer TO test.view AS
                SELECT * FROM test.kafka
                WHERE NOT sleepEachRow(0.25);
        """
        )

        instance.wait_for_log_line("Polled batch of 20 messages")
        # the tricky part here is that disconnect should happen after write prefix, but before write suffix
        # we have 0.25 (sleepEachRow) * 20 ( Rows ) = 5 sec window after "Polled batch of 20 messages"
        # while materialized view is working to inject zookeeper failure
        pm.drop_instance_zk_connections(instance)
        instance.wait_for_log_line(
            "Error.*(Connection loss|Coordination::Exception).*while pushing to view"
        )
        pm.heal_all()
        instance.wait_for_log_line("Committed offset 22")

        result = instance.query(
            "SELECT count(), uniqExact(key), max(key) FROM test.view"
        )
        logging.debug(result)

        # kafka_cluster.open_bash_shell('instance')

        instance.query(
            """
            DROP TABLE test.consumer;
            DROP TABLE test.view SYNC;
        """
        )

        assert TSV(result) == TSV("22\t22\t22")


def test_exception_from_destructor(kafka_cluster):
    instance.query(
        """
        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'xyz',
                     kafka_group_name = '',
                     kafka_commit_on_select = 1,
                     kafka_format = 'JSONEachRow';
    """
    )
    instance.query_and_get_error(
        """
        SELECT * FROM test.kafka;
    """
    )
    instance.query(
        """
        DROP TABLE test.kafka;
    """
    )

    instance.query(
        """
        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'xyz',
                     kafka_group_name = '',
                     kafka_format = 'JSONEachRow';
    """
    )
    instance.query(
        """
        DROP TABLE test.kafka;
    """
    )

    # kafka_cluster.open_bash_shell('instance')
    assert TSV(instance.query("SELECT 1")) == TSV("1")


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_commits_of_unprocessed_messages_on_drop(kafka_cluster, create_query_generator):
    topic_name = "commits_of_unprocessed_messages_on_drop" + k.get_topic_postfix(
        create_query_generator
    )
    messages = [json.dumps({"key": j + 1, "value": j + 1}) for j in range(1)]

    k.kafka_produce(kafka_cluster, topic_name, messages)

    create_query = create_query_generator(
        "kafka",
        "key UInt64, value UInt64",
        topic_list=topic_name,
        consumer_group=f"{topic_name}_test_group",
        settings={
            "kafka_max_block_size": 1000,
            "kafka_flush_interval_ms": 1000,
        },
    )
    instance.query(
        f"""
        DROP TABLE IF EXISTS test.destination SYNC;
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

        {create_query};

        CREATE MATERIALIZED VIEW test.kafka_consumer TO test.destination AS
            SELECT
            key,
            value,
            _topic,
            _key,
            _offset,
            _partition,
            _timestamp
        FROM test.kafka;
    """
    )

    # Waiting for test.kafka_consumer to start consume
    instance.wait_for_log_line("Committed offset [0-9]+")

    cancel = threading.Event()

    i = [2]

    def produce():
        while not cancel.is_set():
            messages = []
            for _ in range(113):
                messages.append(json.dumps({"key": i[0], "value": i[0]}))
                i[0] += 1
            k.kafka_produce(kafka_cluster, topic_name, messages)
            time.sleep(0.5)

    kafka_thread = threading.Thread(target=produce)
    kafka_thread.start()
    time.sleep(4)

    instance.query(
        """
        DROP TABLE test.kafka SYNC;
    """
    )

    new_create_query = create_query_generator(
        "kafka",
        "key UInt64, value UInt64",
        topic_list=topic_name,
        consumer_group=f"{topic_name}_test_group",
        settings={
            "kafka_max_block_size": 10000,
            "kafka_flush_interval_ms": 1000,
        },
    )
    instance.query(new_create_query)

    cancel.set()
    instance.wait_for_log_line("kafka.*Stalled", repetitions=5)

    # kafka_cluster.open_bash_shell('instance')
    # SELECT key, _timestamp, _offset FROM test.destination where runningDifference(key) <> 1 ORDER BY key;

    result = instance.query(
        "SELECT count(), uniqExact(key), max(key) FROM test.destination"
    )
    logging.debug(result)

    instance.query(
        """
        DROP TABLE test.kafka_consumer SYNC;
        DROP TABLE test.destination SYNC;
    """
    )

    kafka_thread.join()
    assert TSV(result) == TSV("{0}\t{0}\t{0}".format(i[0] - 1)), "Missing data!"


# if we came to partition end we will repeat polling until reaching kafka_max_block_size or flush_interval
# that behavior is a bit questionable - we can just take a bigger pauses between polls instead -
# to do more job in a single pass, and give more rest for a thread.
# But in cases of some peaky loads in kafka topic the current contract sounds more predictable and
# easier to understand, so let's keep it as is for now.
# also we can came to eof because we drained librdkafka internal queue too fast
@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_premature_flush_on_eof(kafka_cluster, create_query_generator):
    topic_name = "premature_flush_on_eof" + k.get_topic_postfix(create_query_generator)
    create_query = create_query_generator(
        "kafka",
        "key UInt64, value UInt64",
        topic_list=topic_name,
        consumer_group=topic_name,
    )
    instance.query(
        f"""
        {create_query};
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

    # messages created here will be consumed immediately after MV creation
    # reaching topic EOF.
    # But we should not do flush immediately after reaching EOF, because
    # next poll can return more data, and we should respect kafka_flush_interval_ms
    # and try to form bigger block
    messages = [json.dumps({"key": 1, "value": 1})]
    k.kafka_produce(kafka_cluster, topic_name, messages)

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.kafka_consumer TO test.destination AS
        SELECT
            key,
            value,
            _topic,
            _key,
            _offset,
            _partition,
            _timestamp
        FROM test.kafka;
    """
    )

    # all subscriptions/assignments done during select, so it start sending data to test.destination
    # immediately after creation of MV

    instance.wait_for_log_line("Polled batch of 1 messages")
    instance.wait_for_log_line("Stalled")

    # produce more messages after delay
    k.kafka_produce(kafka_cluster, topic_name, messages)

    # data was not flushed yet (it will be flushed 7.5 sec after creating MV)
    assert int(instance.query("SELECT count() FROM test.destination")) == 0

    instance.wait_for_log_line("Committed offset 2")

    # it should be single part, i.e. single insert
    result = instance.query(
        "SELECT _part, count() FROM test.destination group by _part"
    )
    assert TSV(result) == TSV("all_1_1_0\t2")

    instance.query(
        """
        DROP TABLE test.kafka_consumer;
        DROP TABLE test.destination;
    """
    )


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_issue14202(kafka_cluster, create_query_generator):
    """
    INSERT INTO Kafka Engine from an empty SELECT sub query was leading to failure
    """

    topic_name = "issue14202" + k.get_topic_postfix(create_query_generator)
    create_query = create_query_generator(
        "kafka_q",
        "t UInt64, some_string String",
        topic_list=topic_name,
        consumer_group=topic_name,
    )
    instance.query(
        f"""
        CREATE TABLE test.empty_table (
            dt Date,
            some_string String
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(dt)
        ORDER BY some_string;

        {create_query};
        """
    )

    instance.query(
        "INSERT INTO test.kafka_q SELECT t, some_string  FROM ( SELECT dt AS t, some_string FROM test.empty_table )"
    )
    # check instance is alive
    assert TSV(instance.query("SELECT 1")) == TSV("1")
    instance.query(
        """
        DROP TABLE test.empty_table;
        DROP TABLE test.kafka_q;
    """
    )


def test_kafka_csv_with_thread_per_consumer(kafka_cluster):
    instance.query(
        """
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'csv_with_thread_per_consumer',
                     kafka_group_name = 'csv_with_thread_per_consumer',
                     kafka_format = 'CSV',
                     kafka_row_delimiter = '\\n',
                     kafka_num_consumers = 4,
                     kafka_commit_on_select = 1,
                     kafka_thread_per_consumer = 1;
        """
    )

    messages = []
    for i in range(50):
        messages.append("{i}, {i}".format(i=i))
    k.kafka_produce(kafka_cluster, "csv_with_thread_per_consumer", messages)

    result = ""
    while True:
        result += instance.query("SELECT * FROM test.kafka", ignore_error=True)
        if k.kafka_check_result(result):
            break

    k.kafka_check_result(result, True)


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_engine_put_errors_to_stream(kafka_cluster, create_query_generator):
    topic_name = "kafka_engine_put_errors_to_stream" + k.get_topic_postfix(
        create_query_generator
    )
    create_query = create_query_generator(
        "kafka",
        "i Int64, s String",
        topic_list=topic_name,
        consumer_group=topic_name,
        settings={
            "kafka_max_block_size": 128,
            "kafka_handle_error_mode": "stream",
        },
    )
    instance.query(
        f"""
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.kafka_data;
        DROP TABLE IF EXISTS test.kafka_errors;
        {create_query};
        CREATE MATERIALIZED VIEW test.kafka_data (i Int64, s String)
            ENGINE = MergeTree
            ORDER BY i
            AS SELECT i, s FROM test.kafka WHERE length(_error) == 0;
        CREATE MATERIALIZED VIEW test.kafka_errors (topic String, partition Int64, offset Int64, raw String, error String)
            ENGINE = MergeTree
            ORDER BY (topic, offset)
            AS SELECT
               _topic AS topic,
               _partition AS partition,
               _offset AS offset,
               _raw_message AS raw,
               _error AS error
               FROM test.kafka WHERE length(_error) > 0;
        """
    )

    messages = []
    for i in range(128):
        if i % 2 == 0:
            messages.append(json.dumps({"i": i, "s": k.random_string(8)}))
        else:
            # Unexpected json content for table test.kafka.
            messages.append(
                json.dumps({"i": "n_" + k.random_string(4), "s": k.random_string(8)})
            )

    k.kafka_produce(kafka_cluster, topic_name, messages)
    with k.existing_kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        instance.wait_for_log_line("Committed offset 128")

        assert TSV(instance.query("SELECT count() FROM test.kafka_data")) == TSV("64")
        assert TSV(instance.query("SELECT count() FROM test.kafka_errors")) == TSV("64")

        instance.query(
            """
            DROP TABLE test.kafka;
            DROP TABLE test.kafka_data;
            DROP TABLE test.kafka_errors;
        """
        )


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_engine_put_errors_to_stream_with_random_malformed_json(
    kafka_cluster, create_query_generator
):
    topic_name = (
        "kafka_engine_put_errors_to_stream_with_random_malformed_json"
        + k.get_topic_postfix(create_query_generator)
    )
    create_query = create_query_generator(
        "kafka",
        "i Int64, s String",
        topic_list=topic_name,
        consumer_group=topic_name,
        settings={
            "kafka_max_block_size": 100,
            "kafka_poll_max_batch_size": 1,
            "kafka_handle_error_mode": "stream",
        },
    )

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.kafka_data;
        DROP TABLE IF EXISTS test.kafka_errors;
        {create_query};
        CREATE MATERIALIZED VIEW test.kafka_data (i Int64, s String)
            ENGINE = MergeTree
            ORDER BY i
            AS SELECT i, s FROM test.kafka WHERE length(_error) == 0;
        CREATE MATERIALIZED VIEW test.kafka_errors (topic String, partition Int64, offset Int64, raw String, error String)
            ENGINE = MergeTree
            ORDER BY (topic, offset)
            AS SELECT
               _topic AS topic,
               _partition AS partition,
               _offset AS offset,
               _raw_message AS raw,
               _error AS error
               FROM test.kafka WHERE length(_error) > 0;
        """
    )

    messages = []
    for i in range(128):
        if i % 2 == 0:
            messages.append(k.gen_message_with_jsons(10, 1))
        else:
            messages.append(k.gen_message_with_jsons(10, 0))

    k.kafka_produce(kafka_cluster, topic_name, messages)
    with k.existing_kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        instance.wait_for_log_line("Committed offset 128")
        # 64 good messages, each containing 10 rows
        assert TSV(instance.query("SELECT count() FROM test.kafka_data")) == TSV("640")
        # 64 bad messages, each containing some broken row
        assert TSV(instance.query("SELECT count() FROM test.kafka_errors")) == TSV("64")

        instance.query(
            """
            DROP TABLE test.kafka;
            DROP TABLE test.kafka_data;
            DROP TABLE test.kafka_errors;
        """
        )


def test_kafka_predefined_configuration(kafka_cluster):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )
    topic_name = "conf"
    k.kafka_create_topic(admin_client, topic_name)

    messages = []
    for i in range(50):
        messages.append("{i}, {i}".format(i=i))
    k.kafka_produce(kafka_cluster, topic_name, messages)

    instance.query(
        f"""
        CREATE TABLE test.kafka (key UInt64, value UInt64) ENGINE = Kafka(kafka1, kafka_format='CSV');
        """
    )

    result = ""
    while True:
        result += instance.query("SELECT * FROM test.kafka", ignore_error=True)
        if k.kafka_check_result(result):
            break
    k.kafka_check_result(result, True)


# https://github.com/ClickHouse/ClickHouse/issues/26643
@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_issue26643(kafka_cluster, create_query_generator):
    producer = KafkaProducer(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port),
        value_serializer=k.producer_serializer,
    )
    topic_name = "test_issue26643" + k.get_topic_postfix(create_query_generator)
    thread_per_consumer = k.must_use_thread_per_consumer(create_query_generator)

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        msg = message_with_repeated_pb2.Message(
            tnow=1629000000,
            server="server1",
            clien="host1",
            sPort=443,
            cPort=50000,
            r=[
                message_with_repeated_pb2.dd(
                    name="1", type=444, ttl=123123, data=b"adsfasd"
                ),
                message_with_repeated_pb2.dd(name="2"),
            ],
            method="GET",
        )

        data = b""
        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg

        msg = message_with_repeated_pb2.Message(tnow=1629000002)

        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg

        producer.send(topic_name, value=data)

        data = _VarintBytes(len(serialized_msg)) + serialized_msg
        producer.send(topic_name, value=data)
        producer.flush()

        create_query = create_query_generator(
            "test_queue",
            """`tnow` UInt32,
               `server` String,
               `client` String,
               `sPort` UInt16,
               `cPort` UInt16,
               `r.name` Array(String),
               `r.class` Array(UInt16),
               `r.type` Array(UInt16),
               `r.ttl` Array(UInt32),
               `r.data` Array(String),
               `method` String""",
            topic_list=topic_name,
            consumer_group=f"{topic_name}_group",
            format="Protobuf",
            settings={
                "kafka_schema": "message_with_repeated.proto:Message",
                "kafka_skip_broken_messages": 10000,
                "kafka_thread_per_consumer": thread_per_consumer,
            },
        )

        instance.query(
            f"""
            {create_query};

            SET allow_suspicious_low_cardinality_types=1;

            CREATE TABLE test.log
            (
                `tnow` DateTime('Asia/Istanbul') CODEC(DoubleDelta, LZ4),
                `server` LowCardinality(String),
                `client` LowCardinality(String),
                `sPort` LowCardinality(UInt16),
                `cPort` UInt16 CODEC(T64, LZ4),
                `r.name` Array(String),
                `r.class` Array(LowCardinality(UInt16)),
                `r.type` Array(LowCardinality(UInt16)),
                `r.ttl` Array(LowCardinality(UInt32)),
                `r.data` Array(String),
                `method` LowCardinality(String)
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMMDD(tnow)
            ORDER BY (tnow, server)
            TTL toDate(tnow) + toIntervalMonth(1000)
            SETTINGS index_granularity = 16384, merge_with_ttl_timeout = 7200;

            CREATE MATERIALIZED VIEW test.test_consumer TO test.log AS
            SELECT
                toDateTime(a.tnow) AS tnow,
                a.server AS server,
                a.client AS client,
                a.sPort AS sPort,
                a.cPort AS cPort,
                a.`r.name` AS `r.name`,
                a.`r.class` AS `r.class`,
                a.`r.type` AS `r.type`,
                a.`r.ttl` AS `r.ttl`,
                a.`r.data` AS `r.data`,
                a.method AS method
            FROM test.test_queue AS a;
            """
        )

        instance.wait_for_log_line("Committed offset")
        result = instance.query("SELECT * FROM test.log")

        expected = """\
    2021-08-15 07:00:00	server1		443	50000	['1','2']	[0,0]	[444,0]	[123123,0]	['adsfasd','']	GET
    2021-08-15 07:00:02			0	0	[]	[]	[]	[]	[]
    2021-08-15 07:00:02			0	0	[]	[]	[]	[]	[]
    """
        assert TSV(result) == TSV(expected)


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_num_consumers_limit(kafka_cluster, create_query_generator):
    instance.query("DROP TABLE IF EXISTS test.kafka")

    thread_per_consumer = k.must_use_thread_per_consumer(create_query_generator)

    create_query = create_query_generator(
        "kafka",
        "key UInt64, value UInt64",
        settings={
            "kafka_num_consumers": 100,
            "kafka_thread_per_consumer": thread_per_consumer,
        },
    )
    error = instance.query_and_get_error(create_query)

    assert (
        "BAD_ARGUMENTS" in error
        and "The number of consumers can not be bigger than" in error
    )

    instance.query(
        f"""
        SET kafka_disable_num_consumers_limit = 1;
        {create_query};
        """
    )

    instance.query("DROP TABLE test.kafka")


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_format_with_prefix_and_suffix(kafka_cluster, create_query_generator):
    topic_name = "custom" + k.get_topic_postfix(create_query_generator)

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        create_query = create_query_generator(
            "kafka",
            "key UInt64, value UInt64",
            topic_list=topic_name,
            consumer_group=topic_name,
            format="CustomSeparated",
        )
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.kafka;
            {create_query};
            """
        )

        instance.query(
            "INSERT INTO test.kafka select number*10 as key, number*100 as value from numbers(2) settings format_custom_result_before_delimiter='<prefix>\n', format_custom_result_after_delimiter='<suffix>\n'"
        )

        message_count = 2
        messages = k.kafka_consume_with_retry(kafka_cluster, topic_name, message_count)

        assert len(messages) == 2

        assert (
            "".join(messages)
            == "<prefix>\n0\t0\n<suffix>\n<prefix>\n10\t100\n<suffix>\n"
        )


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_max_rows_per_message(kafka_cluster, create_query_generator):
    topic_name = "custom_max_rows_per_message" + k.get_topic_postfix(
        create_query_generator
    )

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        num_rows = 5

        create_query = create_query_generator(
            "kafka",
            "key UInt64, value UInt64",
            topic_list=topic_name,
            consumer_group=topic_name,
            format="CustomSeparated",
            settings={
                "format_custom_result_before_delimiter": "<prefix>\n",
                "format_custom_result_after_delimiter": "<suffix>\n",
                "kafka_max_rows_per_message": 3,
            },
        )
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.view;
            DROP TABLE IF EXISTS test.kafka;
            {create_query};

            CREATE MATERIALIZED VIEW test.view ENGINE=MergeTree ORDER BY (key, value) AS
                SELECT key, value FROM test.kafka;
        """
        )

        instance.query(
            f"INSERT INTO test.kafka select number*10 as key, number*100 as value from numbers({num_rows}) settings format_custom_result_before_delimiter='<prefix>\n', format_custom_result_after_delimiter='<suffix>\n'"
        )

        message_count = 2
        messages = k.kafka_consume_with_retry(kafka_cluster, topic_name, message_count)

        assert len(messages) == message_count

        assert (
            "".join(messages)
            == "<prefix>\n0\t0\n10\t100\n20\t200\n<suffix>\n<prefix>\n30\t300\n40\t400\n<suffix>\n"
        )

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res) == num_rows,
        )

        result = instance.query("SELECT * FROM test.view")
        assert result == "0\t0\n10\t100\n20\t200\n30\t300\n40\t400\n"


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_block_based_formats_1(kafka_cluster, create_query_generator):
    topic_name = "pretty_space" + k.get_topic_postfix(create_query_generator)

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        create_query = create_query_generator(
            "kafka",
            "key UInt64, value UInt64",
            topic_list=topic_name,
            consumer_group=topic_name,
            format="PrettySpace",
        )
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.kafka;

            {create_query};

            INSERT INTO test.kafka SELECT number * 10 as key, number * 100 as value FROM numbers(5) settings max_block_size=2, optimize_trivial_insert_select=0, output_format_pretty_color=1, output_format_pretty_row_numbers=0;
        """
        )

        message_count = 3
        messages = k.kafka_consume_with_retry(kafka_cluster, topic_name, message_count)
        assert len(messages) == 3

        data = []
        for message in messages:
            split = message.split("\n")
            assert split[0] == " \x1b[1mkey\x1b[0m   \x1b[1mvalue\x1b[0m"
            assert split[1] == ""
            assert split[-1] == ""
            data += [line.split() for line in split[2:-1]]

        assert data == [
            ["0", "0"],
            ["10", "100"],
            ["20", "200"],
            ["30", "300"],
            ["40", "400"],
        ]


def test_system_kafka_consumers(kafka_cluster):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    topic = "system_kafka_cons"
    k.kafka_create_topic(admin_client, topic)

    # Check that format_csv_delimiter parameter works now - as part of all available format settings.
    k.kafka_produce(
        kafka_cluster,
        topic,
        ["1|foo", "2|bar", "42|answer", "100|multi\n101|row\n103|message"],
    )

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.kafka;

        CREATE TABLE test.kafka (a UInt64, b String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic}',
                     kafka_group_name = '{topic}',
                     kafka_commit_on_select = 1,
                     kafka_format = 'CSV',
                     kafka_row_delimiter = '\\n',
                     format_csv_delimiter = '|';
        """
    )

    result = instance.query("SELECT * FROM test.kafka ORDER BY a;")

    check_query = """
        create or replace function stable_timestamp as
          (d)->multiIf(d==toDateTime('1970-01-01 00:00:00'), 'never', abs(dateDiff('second', d, now())) < 30, 'now', toString(d));

        -- check last_used stores microseconds correctly
        create or replace function check_last_used as
          (v) -> if(abs(toStartOfSecond(last_used) - last_used) * 1e6 > 0, 'microseconds', toString(v));

        SELECT database, table, length(consumer_id), assignments.topic, assignments.partition_id,
          assignments.current_offset,
          if(length(exceptions.time)>0, exceptions.time[1]::String, 'never') as last_exception_time_,
          if(length(exceptions.text)>0, exceptions.text[1], 'no exception') as last_exception_,
          stable_timestamp(last_poll_time) as last_poll_time_, num_messages_read, stable_timestamp(last_commit_time) as last_commit_time_,
          num_commits, stable_timestamp(last_rebalance_time) as last_rebalance_time_,
          num_rebalance_revocations, num_rebalance_assignments, is_currently_used,
          check_last_used(last_used) as last_used_,
          if(toStartOfDay(last_used) == toStartOfDay(last_poll_time), 'equal', toString(last_used)) as last_used_and_last_poll_time
          FROM system.kafka_consumers WHERE database='test' and table='kafka' format Vertical;
        """
    assert_eq_with_retry(
        instance,
        check_query,
        """Row 1:
──────
database:                     test
table:                        kafka
length(consumer_id):          67
assignments.topic:            ['system_kafka_cons']
assignments.partition_id:     [0]
assignments.current_offset:   [4]
last_exception_time_:         never
last_exception_:              no exception
last_poll_time_:              now
num_messages_read:            4
last_commit_time_:            now
num_commits:                  1
last_rebalance_time_:         never
num_rebalance_revocations:    0
num_rebalance_assignments:    1
is_currently_used:            0
last_used_:                   microseconds
last_used_and_last_poll_time: equal
""",
    )

    instance.query("DROP TABLE test.kafka")
    k.kafka_delete_topic(admin_client, topic)


def test_system_kafka_consumers_rebalance(kafka_cluster, max_retries=15):
    # based on test_kafka_consumer_hang2
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    producer = KafkaProducer(
        bootstrap_servers="localhost:{}".format(cluster.kafka_port),
        value_serializer=k.producer_serializer,
        key_serializer=k.producer_serializer,
    )

    topic = "system_kafka_cons2"
    k.kafka_create_topic(admin_client, topic, num_partitions=2)

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.kafka2;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic}',
                     kafka_group_name = '{topic}',
                     kafka_commit_on_select = 1,
                     kafka_format = 'JSONEachRow';

        CREATE TABLE test.kafka2 (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic}',
                     kafka_commit_on_select = 1,
                     kafka_group_name = '{topic}',
                     kafka_format = 'JSONEachRow';
        """
    )

    producer.send(topic=topic, value=json.dumps({"key": 1, "value": 1}), partition=0)
    producer.send(topic=topic, value=json.dumps({"key": 11, "value": 11}), partition=1)
    time.sleep(3)

    # first consumer subscribe the topic, try to poll some data, and go to rest
    instance.query("SELECT * FROM test.kafka")

    # second consumer do the same leading to rebalance in the first
    # consumer, try to poll some data
    instance.query("SELECT * FROM test.kafka2")

    producer.send(topic=topic, value=json.dumps({"key": 1, "value": 1}), partition=0)
    producer.send(topic=topic, value=json.dumps({"key": 10, "value": 10}), partition=1)
    time.sleep(3)

    instance.query("SELECT * FROM test.kafka")
    instance.query("SELECT * FROM test.kafka2")
    instance.query("SELECT * FROM test.kafka")
    instance.query("SELECT * FROM test.kafka2")

    result_system_kafka_consumers = instance.query(
        """
        create or replace function stable_timestamp as
          (d)->multiIf(d==toDateTime('1970-01-01 00:00:00'), 'never', abs(dateDiff('second', d, now())) < 30, 'now', toString(d));
        SELECT database, table, length(consumer_id), assignments.topic, assignments.partition_id,
          assignments.current_offset,
          if(length(exceptions.time)>0, exceptions.time[1]::String, 'never') as last_exception_time_,
          if(length(exceptions.text)>0, exceptions.text[1], 'no exception') as last_exception_,
          stable_timestamp(last_poll_time) as last_poll_time_, stable_timestamp(last_commit_time) as last_commit_time_,
          num_commits, stable_timestamp(last_rebalance_time) as last_rebalance_time_,
          num_rebalance_revocations, num_rebalance_assignments, is_currently_used
          FROM system.kafka_consumers WHERE database='test' and table IN ('kafka', 'kafka2') format Vertical;
        """
    )
    logging.debug(f"result_system_kafka_consumers: {result_system_kafka_consumers}")
    assert (
        result_system_kafka_consumers
        == """Row 1:
──────
database:                   test
table:                      kafka
length(consumer_id):        67
assignments.topic:          ['system_kafka_cons2']
assignments.partition_id:   [0]
assignments.current_offset: [2]
last_exception_time_:       never
last_exception_:            no exception
last_poll_time_:            now
last_commit_time_:          now
num_commits:                2
last_rebalance_time_:       now
num_rebalance_revocations:  1
num_rebalance_assignments:  2
is_currently_used:          0

Row 2:
──────
database:                   test
table:                      kafka2
length(consumer_id):        68
assignments.topic:          ['system_kafka_cons2']
assignments.partition_id:   [1]
assignments.current_offset: [2]
last_exception_time_:       never
last_exception_:            no exception
last_poll_time_:            now
last_commit_time_:          now
num_commits:                1
last_rebalance_time_:       never
num_rebalance_revocations:  0
num_rebalance_assignments:  1
is_currently_used:          0
"""
    )

    instance.query("DROP TABLE test.kafka")
    instance.query("DROP TABLE test.kafka2")

    k.kafka_delete_topic(admin_client, topic)


def test_system_kafka_consumers_rebalance_mv(kafka_cluster, max_retries=15):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    producer = KafkaProducer(
        bootstrap_servers="localhost:{}".format(cluster.kafka_port),
        value_serializer=k.producer_serializer,
        key_serializer=k.producer_serializer,
    )

    topic = "system_kafka_cons_mv"
    k.kafka_create_topic(admin_client, topic, num_partitions=2)

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.kafka2;
        DROP TABLE IF EXISTS test.kafka_persistent;
        DROP TABLE IF EXISTS test.kafka_persistent2;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic}',
                     kafka_group_name = '{topic}',
                     kafka_commit_on_select = 1,
                     kafka_format = 'JSONEachRow';

        CREATE TABLE test.kafka2 (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic}',
                     kafka_commit_on_select = 1,
                     kafka_group_name = '{topic}',
                     kafka_format = 'JSONEachRow';

        CREATE TABLE test.kafka_persistent (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE TABLE test.kafka_persistent2 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;

        CREATE MATERIALIZED VIEW test.persistent_kafka_mv TO test.kafka_persistent AS
        SELECT key, value
        FROM test.kafka;

        CREATE MATERIALIZED VIEW test.persistent_kafka_mv2 TO test.kafka_persistent2 AS
        SELECT key, value
        FROM test.kafka2;
        """
    )

    producer.send(topic=topic, value=json.dumps({"key": 1, "value": 1}), partition=0)
    producer.send(topic=topic, value=json.dumps({"key": 11, "value": 11}), partition=1)
    time.sleep(3)

    retries = 0
    result_rdkafka_stat = ""
    while True:
        result_rdkafka_stat = instance.query(
            """
            SELECT table, JSONExtractString(rdkafka_stat, 'type') AS value
            FROM system.kafka_consumers WHERE database='test' and table = 'kafka' format Vertical;
            """
        )
        if result_rdkafka_stat.find("consumer") or retries > max_retries:
            break
        retries += 1
        time.sleep(1)

    assert (
        result_rdkafka_stat
        == """Row 1:
──────
table: kafka
value: consumer
"""
    )

    instance.query("DROP TABLE test.kafka")
    instance.query("DROP TABLE test.kafka2")
    instance.query("DROP TABLE test.kafka_persistent")
    instance.query("DROP TABLE test.kafka_persistent2")
    instance.query("DROP TABLE test.persistent_kafka_mv")
    instance.query("DROP TABLE test.persistent_kafka_mv2")

    k.kafka_delete_topic(admin_client, topic)


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_multiple_read_in_materialized_views(kafka_cluster, create_query_generator):
    topic_name = "multiple_read_from_mv" + k.get_topic_postfix(create_query_generator)

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        create_query = create_query_generator(
            "kafka_multiple_read_input",
            "id Int64",
            topic_list=topic_name,
            consumer_group=topic_name,
        )
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.kafka_multiple_read_input SYNC;
            DROP TABLE IF EXISTS test.kafka_multiple_read_table;
            DROP TABLE IF EXISTS test.kafka_multiple_read_mv;

            {create_query};

            CREATE TABLE test.kafka_multiple_read_table (id Int64)
            ENGINE = MergeTree
            ORDER BY id;


            CREATE MATERIALIZED VIEW test.kafka_multiple_read_mv TO test.kafka_multiple_read_table AS
            SELECT id
            FROM test.kafka_multiple_read_input
            WHERE id NOT IN (
                SELECT id
                FROM test.kafka_multiple_read_table
                WHERE id IN (
                    SELECT id
                    FROM test.kafka_multiple_read_input
                )
            );
            """
        )

        k.kafka_produce(
            kafka_cluster, topic_name, [json.dumps({"id": 42}), json.dumps({"id": 43})]
        )

        expected_result = "42\n43\n"
        res = instance.query_with_retry(
            f"SELECT id FROM test.kafka_multiple_read_table ORDER BY id",
            check_callback=lambda res: res == expected_result,
        )
        assert res == expected_result

        # Verify that the query deduplicates the records as it meant to be
        messages = []
        for _ in range(0, 10):
            messages.append(json.dumps({"id": 42}))
            messages.append(json.dumps({"id": 43}))

        messages.append(json.dumps({"id": 44}))

        k.kafka_produce(kafka_cluster, topic_name, messages)

        expected_result = "42\n43\n44\n"
        res = instance.query_with_retry(
            f"SELECT id FROM test.kafka_multiple_read_table ORDER BY id",
            check_callback=lambda res: res == expected_result,
        )
        assert res == expected_result

        instance.query(
            f"""
            DROP TABLE test.kafka_multiple_read_input;
            DROP TABLE test.kafka_multiple_read_table;
            DROP TABLE test.kafka_multiple_read_mv;
            """
        )


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_null_message(kafka_cluster, create_query_generator):
    topic_name = "null_message"

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.null_message_view;
        DROP TABLE IF EXISTS test.null_message_consumer;
        DROP TABLE IF EXISTS test.null_message_kafka;

        {create_query_generator("null_message_kafka", "value UInt64", topic_list=topic_name, consumer_group="mv")};
        CREATE TABLE test.null_message_view (value UInt64)
            ENGINE = MergeTree()
            ORDER BY value;
        CREATE MATERIALIZED VIEW test.null_message_consumer TO test.null_message_view AS
            SELECT * FROM test.null_message_kafka;
    """
    )

    message_key_values = []
    for i in range(5):
        # Here the key is key for Kafka message
        message = json.dumps({"value": i}) if i != 3 else None
        message_key_values.append({"key": f"{i}".encode(), "message": message})

    producer = k.get_kafka_producer(kafka_cluster.kafka_port, k.producer_serializer, 15)
    for message_kv in message_key_values:
        producer.send(
            topic=topic_name, key=message_kv["key"], value=message_kv["message"]
        )
        producer.flush()

    expected = TSV(
        """
0
1
2
4
"""
    )
    with k.existing_kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        result = instance.query_with_retry(
            "SELECT * FROM test.null_message_view",
            check_callback=lambda res: TSV(res) == expected,
        )

        assert expected == TSV(result)

        instance.query(
            """
            DROP TABLE test.null_message_consumer SYNC;
            DROP TABLE test.null_message_view;
            DROP TABLE test.null_message_kafka SYNC;
        """
        )


def test_kafka_json_type(kafka_cluster):
    # Check that matview does respect Kafka SETTINGS
    k.kafka_produce(
        kafka_cluster,
        "test_json_type",
        [
            '{"a" : 1}',
            '{"a" : 2}',
        ],
    )

    instance.query(
        """
        SET enable_json_type = 1;
        CREATE TABLE test.dst (
            a Int64,
        )
        ENGINE = MergeTree()
        ORDER BY tuple();

        CREATE TABLE test.kafka (data JSON)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'test_json_type',
                     kafka_group_name = 'test_json_type',
                     kafka_format = 'JSONAsObject',
                     kafka_row_delimiter = '\\n',
                     kafka_flush_interval_ms=1000;

        CREATE MATERIALIZED VIEW test.kafka_mv TO test.dst AS
        SELECT
            data.a::Int64 as a
        FROM test.kafka;
        """
    )

    while int(instance.query("SELECT count() FROM test.dst")) < 2:
        time.sleep(1)

    result = instance.query("SELECT * FROM test.dst ORDER BY a;")

    instance.query(
        """
        DROP TABLE test.dst;
        DROP TABLE test.kafka_mv;
        DROP TABLE test.kafka;
    """
    )

    expected = """\
1
2
"""
    assert TSV(result) == TSV(expected)


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
