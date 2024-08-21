import pytest
import json
import os.path as p
import random
import threading
import time
import logging
import io
import string

import kafka.errors
from helpers.test_tools import TSV
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer, BrokerConnection

# protoc --version
# libprotoc 3.0.0
# # to create kafka_pb2.py
# protoc --python_out=. kafka.proto

from . import message_with_repeated_pb2
from .kafka_fixtures import *


# Tests
def test_kafka_string_field_on_first_position_in_protobuf(kafka_cluster):
    # https://github.com/ClickHouse/ClickHouse/issues/12615
    kafka_produce_protobuf_social(
        kafka_cluster, "string_field_on_first_position_in_protobuf", 0, 20
    )
    kafka_produce_protobuf_social(
        kafka_cluster, "string_field_on_first_position_in_protobuf", 20, 1
    )
    kafka_produce_protobuf_social(
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

    kafka_produce_protobuf_messages_no_delimiters(
        kafka_cluster, "pb_no_delimiter", 0, 20
    )
    kafka_produce_protobuf_messages_no_delimiters(
        kafka_cluster, "pb_no_delimiter", 20, 1
    )
    kafka_produce_protobuf_messages_no_delimiters(
        kafka_cluster, "pb_no_delimiter", 21, 29
    )

    result = ""
    while True:
        result += instance.query("SELECT * FROM test.kafka", ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)

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
    [generate_old_create_table_query, generate_new_create_table_query],
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
    kafka_produce(kafka_cluster, topic_name, messages)

    with existing_kafka_topic(get_admin_client(kafka_cluster), topic_name):
        result = instance.query_with_retry(
            "SELECT * FROM test.view", check_callback=kafka_check_result
        )

        kafka_check_result(result, True)

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
            generate_new_create_table_query,
            r"kafka.*Saved offset [0-9]+ for topic-partition \[recreate_kafka_table:[0-9]+",
        ),
        (
            generate_old_create_table_query,
            "kafka.*Committed offset [0-9]+.*recreate_kafka_table",
        ),
    ],
)
def test_kafka_recreate_kafka_table(kafka_cluster, create_query_generator, log_line):
    """
    Checks that materialized view work properly after dropping and recreating the Kafka table.
    """
    topic_name = "recreate_kafka_table"
    thread_per_consumer = must_use_thread_per_consumer(create_query_generator)

    with kafka_topic(get_admin_client(kafka_cluster), topic_name, num_partitions=6):
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
        kafka_produce(kafka_cluster, "recreate_kafka_table", messages)

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

        kafka_produce(kafka_cluster, "recreate_kafka_table", messages)

        instance.query(create_query)

        instance.wait_for_log_line(
            log_line,
            repetitions=6,
            look_behind_lines=100,
        )

        # data was not flushed yet (it will be flushed 7.5 sec after creating MV)
        assert int(instance.query("SELECT count() FROM test.view")) == 240

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
        (generate_old_create_table_query, "Committed offset {offset}"),
        (
            generate_new_create_table_query,
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

    admin_client = get_admin_client(kafka_cluster)

    for compression_type in supported_compression_types:
        logging.debug(("Check compression {}".format(compression_type)))

        topic_name = "test_librdkafka_compression_{}".format(compression_type)
        topic_config = {"compression.type": compression_type}
        with kafka_topic(admin_client, topic_name, config=topic_config):
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

            kafka_produce(kafka_cluster, topic_name, messages)

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
    [generate_new_create_table_query, generate_old_create_table_query],
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
    kafka_produce(kafka_cluster, topic_name, messages)

    with existing_kafka_topic(get_admin_client(kafka_cluster), topic_name):
        result = instance.query_with_retry(
            "SELECT * FROM test.view",
            check_callback=kafka_check_result,
            retry_count=40,
            sleep_time=0.75,
        )

        instance.query(
            """
            DROP TABLE test.consumer;
            DROP TABLE test.view;
        """
        )

        kafka_check_result(result, True)


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_many_materialized_views(kafka_cluster, create_query_generator):
    topic_name = f"mmv-{get_topic_postfix(create_query_generator)}"
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
    kafka_produce(kafka_cluster, topic_name, messages)

    with existing_kafka_topic(get_admin_client(kafka_cluster), topic_name):
        result1 = instance.query_with_retry(
            "SELECT * FROM test.view1", check_callback=kafka_check_result
        )
        result2 = instance.query_with_retry(
            "SELECT * FROM test.view2", check_callback=kafka_check_result
        )

        instance.query(
            """
            DROP TABLE test.consumer1;
            DROP TABLE test.consumer2;
            DROP TABLE test.view1;
            DROP TABLE test.view2;
        """
        )

        kafka_check_result(result1, True)
        kafka_check_result(result2, True)


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_flush_on_big_message(kafka_cluster, create_query_generator):
    # Create batches of messages of size ~100Kb
    kafka_messages = 1000
    batch_messages = 1000
    topic_name = "flush" + get_topic_postfix(create_query_generator)
    messages = [
        json.dumps({"key": i, "value": "x" * 100}) * batch_messages
        for i in range(kafka_messages)
    ]
    kafka_produce(kafka_cluster, topic_name, messages)

    admin_client = get_admin_client(kafka_cluster)

    with existing_kafka_topic(admin_client, topic_name):
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
    with kafka_topic(get_admin_client(kafka_cluster), "virt1", config=topic_config):
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
        kafka_produce(kafka_cluster, "virt1", [messages], 0)

        messages = ""
        for i in range(25, 50):
            messages += json.dumps({"key": i, "value": i}) + "\n"
        kafka_produce(kafka_cluster, "virt1", [messages], 0)

        result = ""
        while True:
            result += instance.query(
                """SELECT _key, key, _topic, value, _offset, _partition, _timestamp = 0 ? '0000-00-00 00:00:00' : toString(_timestamp) AS _timestamp FROM test.kafka""",
                ignore_error=True,
            )
            if kafka_check_result(result, False, "test_kafka_virtual1.reference"):
                break

        kafka_check_result(result, True, "test_kafka_virtual1.reference")


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
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
    with kafka_topic(get_admin_client(kafka_cluster), topic_name, config=topic_config):
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
        kafka_produce(kafka_cluster, topic_name, messages, 0)

        def check_callback(result):
            return kafka_check_result(result, False, "test_kafka_virtual2.reference")

        result = instance.query_with_retry(
            "SELECT kafka_key, key, topic, value, offset, partition, timestamp FROM test.view ORDER BY kafka_key, key",
            check_callback=check_callback,
        )

        kafka_check_result(result, True, "test_kafka_virtual2.reference")

        instance.query(
            """
            DROP TABLE test.consumer;
            DROP TABLE test.view;
        """
        )


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_insert(kafka_cluster, create_query_generator):
    topic_name = "insert1" + get_topic_postfix(create_query_generator)

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

    with existing_kafka_topic(get_admin_client(kafka_cluster), topic_name):
        insert_with_retry(instance, values)

        messages = kafka_consume_with_retry(kafka_cluster, topic_name, message_count)
        result = "\n".join(messages)
        kafka_check_result(result, True)


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_produce_consume(kafka_cluster, create_query_generator):
    topic_name = "insert2" + get_topic_postfix(create_query_generator)

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

        insert_with_retry(instance, values)

    threads = []
    threads_num = 16
    for _ in range(threads_num):
        threads.append(threading.Thread(target=insert))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    with existing_kafka_topic(get_admin_client(kafka_cluster), topic_name):
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
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_commit_on_block_write(kafka_cluster, create_query_generator):
    topic_name = "block" + get_topic_postfix(create_query_generator)
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
            kafka_produce(kafka_cluster, topic_name, messages)

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
        (generate_old_create_table_query, "kafka.*Committed offset 2.*virt2_[01]"),
        (
            generate_new_create_table_query,
            r"kafka.*Saved offset 2 for topic-partition \[virt2_[01]:[0-9]+",
        ),
    ],
)
def test_kafka_virtual_columns2(kafka_cluster, create_query_generator, log_line):
    admin_client = get_admin_client(kafka_cluster)

    topic_config = {
        # default retention, since predefined timestamp_ms is used.
        "retention.ms": "-1",
    }
    thread_per_consumer = must_use_thread_per_consumer(create_query_generator)
    topic_name_0 = "virt2_0"
    topic_name_1 = "virt2_1"
    consumer_group = "virt2" + get_topic_postfix(create_query_generator)
    with kafka_topic(admin_client, topic_name_0, num_partitions=2, config=topic_config):
        with kafka_topic(
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
                value_serializer=producer_serializer,
                key_serializer=producer_serializer,
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

            members = describe_consumer_group(kafka_cluster, consumer_group)
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
    [(generate_old_create_table_query, True), (generate_new_create_table_query, False)],
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
        (generate_new_create_table_query, "Saved offset 5"),
        (generate_old_create_table_query, "Committed offset 5"),
    ],
)
def test_kafka_produce_key_timestamp(kafka_cluster, create_query_generator, log_line):
    topic_name = "insert3"
    topic_config = {
        # default retention, since predefined timestamp_ms is used.
        "retention.ms": "-1",
    }

    with kafka_topic(get_admin_client(kafka_cluster), topic_name, config=topic_config):
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

        # instance.wait_for_log_line(log_line)

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
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_insert_avro(kafka_cluster, create_query_generator):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )
    topic_config = {
        # default retention, since predefined timestamp_ms is used.
        "retention.ms": "-1",
    }
    topic_name = "avro1" + get_topic_postfix(create_query_generator)
    with kafka_topic(admin_client, topic_name, config=topic_config):
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
        messages = kafka_consume_with_retry(
            kafka_cluster,
            topic_name,
            message_count,
            need_decode=False,
            timestamp=1636505534,
        )

        result = ""
        for a_message in messages:
            result += decode_avro(a_message) + "\n"

        expected_result = """{'key': 0, 'value': 0, '_timestamp': 1636505534}
{'key': 10, 'value': 100, '_timestamp': 1636505534}

{'key': 20, 'value': 200, '_timestamp': 1636505534}
{'key': 30, 'value': 300, '_timestamp': 1636505534}

"""
        assert result == expected_result


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
