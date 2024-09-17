import json
import logging
import random
import string
import threading
import time

import kafka.errors
import pytest
from helpers.cluster import ClickHouseCluster, ClickHouseInstance, is_arm
from helpers.network import PartitionManager
from helpers.test_tools import TSV
from kafka import KafkaAdminClient
from test_storage_kafka.conftest import (
    KAFKA_CONSUMER_GROUP_NEW,
    KAFKA_TOPIC_NEW,
    conftest_cluster,
    init_cluster_and_instance,
)
from test_storage_kafka.kafka_tests_utils import (
    describe_consumer_group,
    existing_kafka_topic,
    generate_new_create_table_query,
    generate_old_create_table_query,
    get_admin_client,
    get_topic_postfix,
    kafka_check_result,
    kafka_consume_with_retry,
    kafka_create_topic,
    kafka_delete_topic,
    kafka_produce,
    kafka_topic,
    must_use_thread_per_consumer,
)

# Skip all tests on ARM
if is_arm():
    pytestmark = pytest.mark.skip


def random_string(size=8):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=size))


def test_formats_errors(kafka_cluster: ClickHouseCluster, instance: ClickHouseInstance):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    formats_to_test = [
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
        "Npy",
        "ParquetMetadata",
        "CapnProto",
        "Protobuf",
        "ProtobufSingle",
        "ProtobufList",
        "DWARF",
        "HiveText",
        "MySQLDump",
    ]

    for format_name in formats_to_test:
        kafka_create_topic(admin_client, format_name)
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

            CREATE MATERIALIZED VIEW test.{table_name}_view ENGINE=MergeTree ORDER BY (key, value) AS
                SELECT key, value FROM test.{table_name};
        """
        )

        kafka_produce(
            kafka_cluster,
            format_name,
            ["Broken message\nBroken message\nBroken message\n"],
        )

    for format_name in formats_to_test:
        table_name = f"kafka_{format_name}"
        num_errors = int(
            instance.query_with_retry(
                f"SELECT length(exceptions.text) from system.kafka_consumers where database = 'test' and table = '{table_name}'",
                check_callback=lambda res: int(res) > 0,
            )
        )

        assert num_errors > 0, f"Failed {format_name}"

        instance.query(f"DROP TABLE test.{table_name}")
        instance.query(f"DROP TABLE test.{table_name}_view")
        kafka_delete_topic(admin_client, format_name)


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_block_based_formats_2(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
    admin_client = get_admin_client(kafka_cluster)
    num_rows = 100
    message_count = 9

    for format_name in [
        "JSONColumns",
        "Native",
        "Arrow",
        "Parquet",
        "ORC",
        "JSONCompactColumns",
    ]:
        topic_name = format_name + get_topic_postfix(create_query_generator)
        table_name = f"kafka_{format_name}"
        logging.debug(f"Checking format {format_name}")
        with kafka_topic(admin_client, topic_name):
            create_query = create_query_generator(
                table_name,
                "key UInt64, value UInt64",
                topic_list=topic_name,
                consumer_group=topic_name,
                format=format_name,
            )

            instance.query(
                f"""
                DROP TABLE IF EXISTS test.view;
                DROP TABLE IF EXISTS test.{table_name};

                {create_query};

                CREATE MATERIALIZED VIEW test.view ENGINE=MergeTree ORDER BY (key, value) AS
                    SELECT key, value FROM test.{table_name};

                INSERT INTO test.{table_name} SELECT number * 10 as key, number * 100 as value FROM numbers({num_rows}) settings max_block_size=12, optimize_trivial_insert_select=0;
            """
            )
            messages = kafka_consume_with_retry(
                kafka_cluster, topic_name, message_count, need_decode=False
            )
            assert len(messages) == message_count

            rows = int(
                instance.query_with_retry(
                    "SELECT count() FROM test.view",
                    check_callback=lambda res: int(res) == num_rows,
                )
            )

            assert rows == num_rows

            result = instance.query("SELECT * FROM test.view ORDER by key")
            expected = ""
            for i in range(num_rows):
                expected += str(i * 10) + "\t" + str(i * 100) + "\n"
            assert result == expected


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_flush_on_big_message(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
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
def test_kafka_recreate_kafka_table(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
    log_line,
):
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
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_commit_on_block_write(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
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


# TODO(antaljanosbenjamin): find another way to make insertion fail
@pytest.mark.parametrize(
    "create_query_generator",
    [
        generate_old_create_table_query,
        # generate_new_create_table_query,
    ],
)
def test_kafka_no_holes_when_write_suffix_failed(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
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

        instance.query(
            """
            DROP TABLE test.consumer;
            DROP TABLE test.view SYNC;
        """
        )

        assert TSV(result) == TSV("22\t22\t22")


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_flush_by_block_size(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
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
        logging.debug(result)

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
def test_max_rows_per_message(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
    topic_name = "custom_max_rows_per_message" + get_topic_postfix(
        create_query_generator
    )

    with kafka_topic(get_admin_client(kafka_cluster), topic_name):
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
        messages = kafka_consume_with_retry(kafka_cluster, topic_name, message_count)

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
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_many_materialized_views(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
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
def test_kafka_engine_put_errors_to_stream(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
    topic_name = "kafka_engine_put_errors_to_stream" + get_topic_postfix(
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
            messages.append(json.dumps({"i": i, "s": random_string(8)}))
        else:
            # Unexpected json content for table test.kafka.
            messages.append(
                json.dumps({"i": "n_" + random_string(4), "s": random_string(8)})
            )

    kafka_produce(kafka_cluster, topic_name, messages)
    with existing_kafka_topic(get_admin_client(kafka_cluster), topic_name):
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
    "create_query_generator, do_direct_read",
    [(generate_old_create_table_query, True), (generate_new_create_table_query, False)],
)
def test_kafka_producer_consumer_separate_settings(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
    do_direct_read,
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


def test_kafka_virtual_columns(
    kafka_cluster: ClickHouseCluster, instance: ClickHouseInstance
):
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


def test_kafka_settings_new_syntax(
    kafka_cluster: ClickHouseCluster, instance: ClickHouseInstance
):
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
    kafka_produce(kafka_cluster, KAFKA_TOPIC_NEW, messages)

    # Insert couple of malformed messages.
    kafka_produce(kafka_cluster, KAFKA_TOPIC_NEW, ["}{very_broken_message,"])
    kafka_produce(kafka_cluster, KAFKA_TOPIC_NEW, ["}another{very_broken_message,"])

    messages = []
    for i in range(25, 50):
        messages.append(json.dumps({"key": i, "value": i}))
    kafka_produce(kafka_cluster, KAFKA_TOPIC_NEW, messages)

    result = ""
    while True:
        result += instance.query("SELECT * FROM test.kafka", ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)

    members = describe_consumer_group(kafka_cluster, KAFKA_CONSUMER_GROUP_NEW)
    assert members[0]["client_id"] == "instance test 1234"


def test_kafka_tsv_with_delimiter(
    kafka_cluster: ClickHouseCluster, instance: ClickHouseInstance
):
    messages = []
    for i in range(50):
        messages.append("{i}\t{i}".format(i=i))
    kafka_produce(kafka_cluster, "tsv", messages)

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
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


def test_kafka_csv_with_delimiter(
    kafka_cluster: ClickHouseCluster, instance: ClickHouseInstance
):
    messages = []
    for i in range(50):
        messages.append("{i}, {i}".format(i=i))
    kafka_produce(kafka_cluster, "csv", messages)

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
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_format_with_prefix_and_suffix(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
    topic_name = "custom" + get_topic_postfix(create_query_generator)

    with kafka_topic(get_admin_client(kafka_cluster), topic_name):
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
        messages = kafka_consume_with_retry(kafka_cluster, topic_name, message_count)

        assert len(messages) == 2

        assert (
            "".join(messages)
            == "<prefix>\n0\t0\n<suffix>\n<prefix>\n10\t100\n<suffix>\n"
        )


if __name__ == "__main__":
    init_cluster_and_instance()
    conftest_cluster.start()
    input("Cluster created, press any key to destroy...")
    conftest_cluster.shutdown()
