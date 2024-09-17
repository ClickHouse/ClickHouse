import io
import json
import logging
import random
import threading
import time

import avro.datafile
import avro.io
import avro.schema
import pytest
from google.protobuf.internal.encoder import _VarintBytes
from helpers.cluster import ClickHouseCluster, ClickHouseInstance, is_arm
from helpers.test_tools import TSV
from kafka import KafkaAdminClient, KafkaProducer
from test_storage_kafka import message_with_repeated_pb2
from test_storage_kafka.conftest import conftest_cluster, init_cluster_and_instance
from test_storage_kafka.kafka_tests_utils import (
    existing_kafka_topic,
    generate_new_create_table_query,
    generate_old_create_table_query,
    get_admin_client,
    get_topic_postfix,
    insert_with_retry,
    kafka_check_result,
    kafka_consume_with_retry,
    kafka_create_topic,
    kafka_delete_topic,
    kafka_produce,
    kafka_produce_protobuf_messages,
    kafka_topic,
    must_use_thread_per_consumer,
    producer_serializer,
)

# Skip all tests on ARM
if is_arm():
    pytestmark = pytest.mark.skip

# TODO: add test for run-time offset update in CH, if we manually update it on Kafka side.
# TODO: add test for SELECT LIMIT is working.


def decode_avro(message):
    b = io.BytesIO(message)
    ret = avro.datafile.DataFileReader(b, avro.io.DatumReader())

    output = io.StringIO()
    for record in ret:
        print(record, file=output)
    return output.getvalue()


@pytest.mark.parametrize(
    "create_query_generator",
    [
        generate_old_create_table_query,
        generate_new_create_table_query,
    ],
)
def test_kafka_consumer_failover(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
    topic_name = "kafka_consumer_failover" + get_topic_postfix(create_query_generator)

    with kafka_topic(get_admin_client(kafka_cluster), topic_name, num_partitions=2):
        consumer_group = f"{topic_name}_group"
        create_queries = []
        for counter in range(3):
            create_queries.append(
                create_query_generator(
                    f"kafka{counter+1}",
                    "key UInt64, value UInt64",
                    topic_list=topic_name,
                    consumer_group=consumer_group,
                    settings={
                        "kafka_max_block_size": 1,
                        "kafka_poll_timeout_ms": 200,
                    },
                )
            )

        instance.query(
            f"""
            {create_queries[0]};
            {create_queries[1]};
            {create_queries[2]};

            CREATE TABLE test.destination (
                key UInt64,
                value UInt64,
                _consumed_by LowCardinality(String)
            )
            ENGINE = MergeTree()
            ORDER BY key;

            CREATE MATERIALIZED VIEW test.kafka1_mv TO test.destination AS
            SELECT key, value, 'kafka1' as _consumed_by
            FROM test.kafka1;

            CREATE MATERIALIZED VIEW test.kafka2_mv TO test.destination AS
            SELECT key, value, 'kafka2' as _consumed_by
            FROM test.kafka2;

            CREATE MATERIALIZED VIEW test.kafka3_mv TO test.destination AS
            SELECT key, value, 'kafka3' as _consumed_by
            FROM test.kafka3;
            """
        )

        producer = KafkaProducer(
            bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port),
            value_serializer=producer_serializer,
            key_serializer=producer_serializer,
        )

        ## all 3 attached, 2 working
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 1, "value": 1}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 1, "value": 1}),
            partition=1,
        )
        producer.flush()

        count_query = "SELECT count() FROM test.destination"
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > 0
        )

        ## 2 attached, 2 working
        instance.query("DETACH TABLE test.kafka1")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 2, "value": 2}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 2, "value": 2}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 1 attached, 1 working
        instance.query("DETACH TABLE test.kafka2")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 3, "value": 3}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 3, "value": 3}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 2 attached, 2 working
        instance.query("ATTACH TABLE test.kafka1")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 4, "value": 4}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 4, "value": 4}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 1 attached, 1 working
        instance.query("DETACH TABLE test.kafka3")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 5, "value": 5}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 5, "value": 5}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 2 attached, 2 working
        instance.query("ATTACH TABLE test.kafka2")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 6, "value": 6}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 6, "value": 6}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 3 attached, 2 working
        instance.query("ATTACH TABLE test.kafka3")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 7, "value": 7}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 7, "value": 7}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 2 attached, same 2 working
        instance.query("DETACH TABLE test.kafka3")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 8, "value": 8}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 8, "value": 8}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_row_based_formats(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
    admin_client = get_admin_client(kafka_cluster)

    formats_to_test = [
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
    ]

    num_rows = 10
    max_rows_per_message = 5
    message_count = num_rows / max_rows_per_message
    expected = ""
    for i in range(num_rows):
        expected += str(i * 10) + "\t" + str(i * 100) + "\n"

    for format_name in formats_to_test:
        logging.debug("Setting up {format_name}")

        topic_name = format_name + get_topic_postfix(create_query_generator)
        table_name = f"kafka_{format_name}"

        kafka_create_topic(admin_client, topic_name)

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
            DROP TABLE IF EXISTS test.{table_name}_view;
            DROP TABLE IF EXISTS test.{table_name};

            {create_query};

            CREATE MATERIALIZED VIEW test.{table_name}_view ENGINE=MergeTree ORDER BY (key, value) AS
                SELECT key, value FROM test.{table_name};
        """
        )

    for format_name in formats_to_test:
        logging.debug("Inserting to {format_name}")

        topic_name = format_name + get_topic_postfix(create_query_generator)
        table_name = f"kafka_{format_name}"
        instance.query(
            f"INSERT INTO test.{table_name} SELECT number * 10 as key, number * 100 as value FROM numbers({num_rows})"
        )

    for format_name in formats_to_test:
        topic_name = format_name + get_topic_postfix(create_query_generator)
        table_name = f"kafka_{format_name}"
        messages = kafka_consume_with_retry(
            kafka_cluster, topic_name, message_count, need_decode=False
        )

        assert len(messages) == message_count

        result = instance.query_with_retry(
            f"SELECT * FROM test.{table_name}_view",
            check_callback=lambda res: res == expected,
        )

        assert result == expected
        kafka_delete_topic(admin_client, topic_name)


def test_kafka_duplicates_when_commit_failed(
    kafka_cluster: ClickHouseCluster, instance: ClickHouseInstance
):
    messages = [json.dumps({"key": j + 1, "value": "x" * 300}) for j in range(22)]
    kafka_produce(kafka_cluster, "duplicates_when_commit_failed", messages)

    instance.query(
        """
        DROP TABLE IF EXISTS test.view SYNC;
        DROP TABLE IF EXISTS test.consumer SYNC;

        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'duplicates_when_commit_failed',
                     kafka_group_name = 'duplicates_when_commit_failed',
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
    kafka_cluster.pause_container("kafka1")

    # if we restore the connection too fast (<30sec) librdkafka will not report any timeout
    # (alternative is to decrease the default session timeouts for librdkafka)
    #
    # when the delay is too long (>50sec) broker will decide to remove us from the consumer group,
    # and will start answering "Broker: Unknown member"
    instance.wait_for_log_line(
        "Exception during commit attempt: Local: Waiting for coordinator", timeout=45
    )
    instance.wait_for_log_line("All commit attempts failed", look_behind_lines=500)

    kafka_cluster.unpause_container("kafka1")

    instance.wait_for_log_line("Committed offset 22")

    result = instance.query("SELECT count(), uniqExact(key), max(key) FROM test.view")
    logging.debug(result)

    instance.query(
        """
        DROP TABLE test.consumer SYNC;
        DROP TABLE test.view SYNC;
    """
    )

    # After https://github.com/edenhill/librdkafka/issues/2631
    # timeout triggers rebalance, making further commits to the topic after getting back online
    # impossible. So we have a duplicate in that scenario, but we report that situation properly.
    assert TSV(result) == TSV("42\t22\t22")


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_commits_of_unprocessed_messages_on_drop(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
    topic_name = "commits_of_unprocessed_messages_on_drop" + get_topic_postfix(
        create_query_generator
    )
    messages = [json.dumps({"key": j + 1, "value": j + 1}) for j in range(1)]

    kafka_produce(kafka_cluster, topic_name, messages)

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
            kafka_produce(kafka_cluster, topic_name, messages)
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


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_produce_consume(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
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
def test_kafka_materialized_view(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
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
    "create_query_generator",
    [generate_new_create_table_query, generate_old_create_table_query],
)
def test_kafka_materialized_view_with_subquery(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
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
def test_kafka_lot_of_partitions_partial_commit_of_bulk(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    topic_name = "topic_with_multiple_partitions2" + get_topic_postfix(
        create_query_generator
    )
    with kafka_topic(admin_client, topic_name):
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
        kafka_produce(kafka_cluster, topic_name, messages)

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


# https://github.com/ClickHouse/ClickHouse/issues/26643
@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_issue26643(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
    producer = KafkaProducer(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port),
        value_serializer=producer_serializer,
    )
    topic_name = "test_issue26643" + get_topic_postfix(create_query_generator)
    thread_per_consumer = must_use_thread_per_consumer(create_query_generator)

    with kafka_topic(get_admin_client(kafka_cluster), topic_name):
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


def test_kafka_csv_with_thread_per_consumer(
    kafka_cluster: ClickHouseCluster, instance: ClickHouseInstance
):
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
    kafka_produce(kafka_cluster, "csv_with_thread_per_consumer", messages)

    result = ""
    while True:
        result += instance.query("SELECT * FROM test.kafka", ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


def test_kafka_select_empty(
    kafka_cluster: ClickHouseCluster, instance: ClickHouseInstance
):
    admin_client = get_admin_client(kafka_cluster)
    topic_name = "empty"
    kafka_create_topic(admin_client, topic_name)

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
    kafka_delete_topic(admin_client, topic_name)


def test_kafka_issue11308(
    kafka_cluster: ClickHouseCluster, instance: ClickHouseInstance
):
    # Check that matview does respect Kafka SETTINGS
    kafka_produce(
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


def test_kafka_protobuf(kafka_cluster: ClickHouseCluster, instance: ClickHouseInstance):
    kafka_produce_protobuf_messages(kafka_cluster, "pb", 0, 20)
    kafka_produce_protobuf_messages(kafka_cluster, "pb", 20, 1)
    kafka_produce_protobuf_messages(kafka_cluster, "pb", 21, 29)

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
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


def test_kafka_predefined_configuration(
    kafka_cluster: ClickHouseCluster, instance: ClickHouseInstance
):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )
    topic_name = "conf"
    kafka_create_topic(admin_client, topic_name)

    messages = []
    for i in range(50):
        messages.append("{i}, {i}".format(i=i))
    kafka_produce(kafka_cluster, topic_name, messages)

    instance.query(
        f"""
        CREATE TABLE test.kafka (key UInt64, value UInt64) ENGINE = Kafka(kafka1, kafka_format='CSV');
        """
    )

    result = ""
    while True:
        result += instance.query("SELECT * FROM test.kafka", ignore_error=True)
        if kafka_check_result(result):
            break
    kafka_check_result(result, True)


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_insert_avro(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
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


def test_exception_from_destructor(
    kafka_cluster: ClickHouseCluster, instance: ClickHouseInstance
):
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

    assert TSV(instance.query("SELECT 1")) == TSV("1")


if __name__ == "__main__":
    init_cluster_and_instance()
    conftest_cluster.start()
    input("Cluster created, press any key to destroy...")
    conftest_cluster.shutdown()
