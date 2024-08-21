import pytest
import json
import os.path as p
import random
import threading
import time
import logging
import io
import string
import ast
import math

from helpers.network import PartitionManager
from helpers.test_tools import TSV
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer, BrokerConnection

# protoc --version
# libprotoc 3.0.0
# # to create kafka_pb2.py
# protoc --python_out=. kafka.proto

from .kafka_fixtures import *


# Tests
@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_produce_consume_avro(kafka_cluster, create_query_generator):
    topic_name = "insert_avro" + get_topic_postfix(create_query_generator)
    with kafka_topic(get_admin_client(kafka_cluster), topic_name):
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
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_flush_by_time(kafka_cluster, create_query_generator):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )
    topic_name = "flush_by_time" + get_topic_postfix(create_query_generator)

    with kafka_topic(admin_client, topic_name):
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
                kafka_produce(kafka_cluster, topic_name, messages)
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
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_flush_by_block_size(kafka_cluster, create_query_generator):
    topic_name = "flush_by_block_size" + get_topic_postfix(create_query_generator)

    cancel = threading.Event()

    def produce():
        while not cancel.is_set():
            messages = []
            messages.append(json.dumps({"key": 0, "value": 0}))
            kafka_produce(kafka_cluster, topic_name, messages)

    kafka_thread = threading.Thread(target=produce)

    with kafka_topic(get_admin_client(kafka_cluster), topic_name):
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
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_lot_of_partitions_partial_commit_of_bulk(
    kafka_cluster, create_query_generator
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


@pytest.mark.parametrize(
    "create_query_generator, log_line",
    [
        (generate_old_create_table_query, "{}.*Polled offset [0-9]+"),
        (generate_new_create_table_query, "{}.*Saved offset"),
    ],
)
def test_kafka_rebalance(kafka_cluster, create_query_generator, log_line):
    NUMBER_OF_CONSURRENT_CONSUMERS = 11

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
    topic_name = "topic_with_multiple_partitions" + get_topic_postfix(
        create_query_generator
    )
    table_name_prefix = "kafka_consumer"
    keeper_path = f"/clickhouse/{{database}}/{table_name_prefix}"
    with kafka_topic(admin_client, topic_name, num_partitions=11):
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
                kafka_produce(kafka_cluster, topic_name, messages)

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


# TODO(antaljanosbenjamin): find another way to make insertion fail
@pytest.mark.parametrize(
    "create_query_generator",
    [
        generate_old_create_table_query,
        # generate_new_create_table_query,
    ],
)
def test_kafka_no_holes_when_write_suffix_failed(kafka_cluster, create_query_generator):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )
    topic_name = "no_holes_when_write_suffix_failed" + get_topic_postfix(
        create_query_generator
    )

    with kafka_topic(admin_client, topic_name):
        messages = [json.dumps({"key": j + 1, "value": "x" * 300}) for j in range(22)]
        kafka_produce(kafka_cluster, topic_name, messages)

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
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_commits_of_unprocessed_messages_on_drop(kafka_cluster, create_query_generator):
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


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_bad_reschedule(kafka_cluster, create_query_generator):
    topic_name = "test_bad_reschedule" + get_topic_postfix(create_query_generator)

    messages = [json.dumps({"key": j + 1, "value": j + 1}) for j in range(20000)]
    kafka_produce(kafka_cluster, topic_name, messages)

    create_query = create_query_generator(
        "kafka",
        "key UInt64, value UInt64",
        topic_list=topic_name,
        consumer_group=topic_name,
        settings={
            "kafka_max_block_size": 1000,
            "kafka_flush_interval_ms": 1000,
        },
    )
    instance.query(
        f"""
        {create_query};

        CREATE MATERIALIZED VIEW test.destination ENGINE=MergeTree ORDER BY tuple() AS
        SELECT
            key,
            now() as consume_ts,
            value,
            _topic,
            _key,
            _offset,
            _partition,
            _timestamp
        FROM test.kafka;
    """
    )

    instance.wait_for_log_line("Committed offset 20000")

    assert (
        int(
            instance.query(
                "SELECT max(consume_ts) - min(consume_ts) FROM test.destination"
            )
        )
        < 8
    )


def test_kafka_duplicates_when_commit_failed(kafka_cluster):
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

    # kafka_cluster.open_bash_shell('instance')
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


# if we came to partition end we will repeat polling until reaching kafka_max_block_size or flush_interval
# that behavior is a bit questionable - we can just take a bigger pauses between polls instead -
# to do more job in a single pass, and give more rest for a thread.
# But in cases of some peaky loads in kafka topic the current contract sounds more predictable and
# easier to understand, so let's keep it as is for now.
# also we can came to eof because we drained librdkafka internal queue too fast
@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_premature_flush_on_eof(kafka_cluster, create_query_generator):
    topic_name = "premature_flush_on_eof" + get_topic_postfix(create_query_generator)
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
    kafka_produce(kafka_cluster, topic_name, messages)

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
    kafka_produce(kafka_cluster, topic_name, messages)

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
    "create_query_generator, do_direct_read",
    [(generate_old_create_table_query, True), (generate_new_create_table_query, False)],
)
def test_kafka_unavailable(kafka_cluster, create_query_generator, do_direct_read):
    number_of_messages = 20000
    topic_name = "test_bad_reschedule" + get_topic_postfix(create_query_generator)
    messages = [
        json.dumps({"key": j + 1, "value": j + 1}) for j in range(number_of_messages)
    ]
    kafka_produce(kafka_cluster, topic_name, messages)

    with existing_kafka_topic(get_admin_client(kafka_cluster), topic_name):
        kafka_cluster.pause_container("kafka1")

        create_query = create_query_generator(
            "test_bad_reschedule",
            "key UInt64, value UInt64",
            topic_list=topic_name,
            consumer_group=topic_name,
            settings={"kafka_max_block_size": 1000},
        )
        instance.query(
            f"""
            {create_query};

            CREATE MATERIALIZED VIEW test.destination_unavailable ENGINE=MergeTree ORDER BY tuple() AS
            SELECT
                key,
                now() as consume_ts,
                value,
                _topic,
                _key,
                _offset,
                _partition,
                _timestamp
            FROM test.test_bad_reschedule;
        """
        )

        if do_direct_read:
            instance.query("SELECT * FROM test.test_bad_reschedule")
        instance.query("SELECT count() FROM test.destination_unavailable")

        # enough to trigger issue
        time.sleep(30)
        kafka_cluster.unpause_container("kafka1")

        result = instance.query_with_retry(
            "SELECT count() FROM test.destination_unavailable",
            sleep_time=1,
            check_callback=lambda res: int(res) == number_of_messages,
        )

        assert int(result) == number_of_messages


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_issue14202(kafka_cluster, create_query_generator):
    """
    INSERT INTO Kafka Engine from an empty SELECT sub query was leading to failure
    """

    topic_name = "issue14202" + get_topic_postfix(create_query_generator)
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
    kafka_produce(kafka_cluster, "csv_with_thread_per_consumer", messages)

    result = ""
    while True:
        result += instance.query("SELECT * FROM test.kafka", ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
