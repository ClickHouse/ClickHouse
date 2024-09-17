import io
import json
import logging
import math
import random
import time

import pytest
from helpers.cluster import ClickHouseCluster, ClickHouseInstance, is_arm
from helpers.test_tools import TSV
from kafka import KafkaAdminClient, KafkaProducer
from test_storage_kafka.conftest import (
    KAFKA_CONSUMER_GROUP_NEW,
    KAFKA_CONSUMER_GROUP_OLD,
    KAFKA_TOPIC_OLD,
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
    insert_with_retry,
    kafka_check_result,
    kafka_consume_with_retry,
    kafka_create_topic,
    kafka_delete_topic,
    kafka_produce,
    kafka_produce_protobuf_social,
    kafka_topic,
    must_use_thread_per_consumer,
    producer_serializer,
)

# Skip all tests on ARM
if is_arm():
    pytestmark = pytest.mark.skip


def gen_normal_json():
    return '{"i":1000, "s":"ABC123abc"}'


def gen_malformed_json():
    return '{"i":"n1000", "s":"1000"}'


def gen_message_with_jsons(jsons=10, malformed=0):
    s = io.StringIO()

    # we don't care on which position error will be added
    # (we skip whole broken message), but we need to be
    # sure that at least one error will be added,
    # otherwise test will fail.
    error_pos = random.randint(0, jsons - 1)

    for i in range(jsons):
        if malformed and i == error_pos:
            s.write(gen_malformed_json())
        else:
            s.write(gen_normal_json())
        s.write(" ")
    return s.getvalue()


@pytest.mark.parametrize(
    "create_query_generator, do_direct_read",
    [(generate_old_create_table_query, True), (generate_new_create_table_query, False)],
)
def test_kafka_unavailable(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
    do_direct_read,
):
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
def test_bad_reschedule(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
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
def test_librdkafka_compression(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
    log_line,
):
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
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_multiple_read_in_materialized_views(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
    topic_name = "multiple_read_from_mv" + get_topic_postfix(create_query_generator)

    with kafka_topic(get_admin_client(kafka_cluster), topic_name):
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

        kafka_produce(
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

        kafka_produce(kafka_cluster, topic_name, messages)

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
    "create_query_generator, log_line",
    [
        (generate_old_create_table_query, "kafka.*Committed offset 2.*virt2_[01]"),
        (
            generate_new_create_table_query,
            r"kafka.*Saved offset 2 for topic-partition \[virt2_[01]:[0-9]+",
        ),
    ],
)
def test_kafka_virtual_columns2(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
    log_line,
):
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
                bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port),
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
    "create_query_generator, log_line",
    [
        (generate_new_create_table_query, "Saved offset 5"),
        (generate_old_create_table_query, "Committed offset 5"),
    ],
)
def test_kafka_produce_key_timestamp(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
    log_line,
):
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
def test_kafka_engine_put_errors_to_stream_with_random_malformed_json(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
    topic_name = (
        "kafka_engine_put_errors_to_stream_with_random_malformed_json"
        + get_topic_postfix(create_query_generator)
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
            messages.append(gen_message_with_jsons(10, 1))
        else:
            messages.append(gen_message_with_jsons(10, 0))

    kafka_produce(kafka_cluster, topic_name, messages)
    with existing_kafka_topic(get_admin_client(kafka_cluster), topic_name):
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


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_produce_consume_avro(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
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
def test_kafka_virtual_columns_with_materialized_view(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
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


def test_system_kafka_consumers_rebalance_mv(
    kafka_cluster: ClickHouseCluster, instance: ClickHouseInstance, max_retries=15
):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    producer = KafkaProducer(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port),
        value_serializer=producer_serializer,
        key_serializer=producer_serializer,
    )

    topic = "system_kafka_cons_mv"
    kafka_create_topic(admin_client, topic, num_partitions=2)

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
            SELECT table, JSONExtractString(rdkafka_stat, 'type')
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
table:                                   kafka
JSONExtractString(rdkafka_stat, 'type'): consumer
"""
    )

    instance.query("DROP TABLE test.kafka")
    instance.query("DROP TABLE test.kafka2")
    instance.query("DROP TABLE test.kafka_persistent")
    instance.query("DROP TABLE test.kafka_persistent2")
    instance.query("DROP TABLE test.persistent_kafka_mv")
    instance.query("DROP TABLE test.persistent_kafka_mv2")

    kafka_delete_topic(admin_client, topic)


def test_system_kafka_consumers(
    kafka_cluster: ClickHouseCluster, instance: ClickHouseInstance
):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    topic = "system_kafka_cons"
    kafka_create_topic(admin_client, topic)

    # Check that format_csv_delimiter parameter works now - as part of all available format settings.
    kafka_produce(
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

    result_system_kafka_consumers = instance.query(
        """
        create or replace function stable_timestamp as
          (d)->multiIf(d==toDateTime('1970-01-01 00:00:00'), 'never', abs(dateDiff('second', d, now())) < 30, 'now', toString(d));

        SELECT database, table, length(consumer_id), assignments.topic, assignments.partition_id,
          assignments.current_offset,
          if(length(exceptions.time)>0, exceptions.time[1]::String, 'never') as last_exception_time_,
          if(length(exceptions.text)>0, exceptions.text[1], 'no exception') as last_exception_,
          stable_timestamp(last_poll_time) as last_poll_time_, num_messages_read, stable_timestamp(last_commit_time) as last_commit_time_,
          num_commits, stable_timestamp(last_rebalance_time) as last_rebalance_time_,
          num_rebalance_revocations, num_rebalance_assignments, is_currently_used
          FROM system.kafka_consumers WHERE database='test' and table='kafka' format Vertical;
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
assignments.topic:          ['system_kafka_cons']
assignments.partition_id:   [0]
assignments.current_offset: [4]
last_exception_time_:       never
last_exception_:            no exception
last_poll_time_:            now
num_messages_read:          4
last_commit_time_:          now
num_commits:                1
last_rebalance_time_:       never
num_rebalance_revocations:  0
num_rebalance_assignments:  1
is_currently_used:          0
"""
    )

    instance.query("DROP TABLE test.kafka")
    kafka_delete_topic(admin_client, topic)


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_insert(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
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


def test_kafka_settings_old_syntax(
    kafka_cluster: ClickHouseCluster, instance: ClickHouseInstance
):
    assert TSV(
        instance.query(
            "SELECT * FROM system.macros WHERE macro like 'kafka%' ORDER BY macro",
            ignore_error=True,
        )
    ) == TSV(
        f"""kafka_broker	kafka1
kafka_client_id	instance
kafka_format_json_each_row	JSONEachRow
kafka_group_name_new	{KAFKA_CONSUMER_GROUP_NEW}
kafka_group_name_old	{KAFKA_CONSUMER_GROUP_OLD}
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
    kafka_produce(kafka_cluster, KAFKA_TOPIC_OLD, messages)

    result = ""
    while True:
        result += instance.query("SELECT * FROM test.kafka", ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)

    members = describe_consumer_group(kafka_cluster, KAFKA_CONSUMER_GROUP_OLD)
    assert members[0]["client_id"] == "ClickHouse-instance-test-kafka"


def test_kafka_json_without_delimiter(
    kafka_cluster: ClickHouseCluster, instance: ClickHouseInstance
):
    messages = ""
    for i in range(25):
        messages += json.dumps({"key": i, "value": i}) + "\n"
    kafka_produce(kafka_cluster, "json", [messages])

    messages = ""
    for i in range(25, 50):
        messages += json.dumps({"key": i, "value": i}) + "\n"
    kafka_produce(kafka_cluster, "json", [messages])

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
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


def test_kafka_string_field_on_first_position_in_protobuf(
    kafka_cluster: ClickHouseCluster, instance: ClickHouseInstance
):
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


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_block_based_formats_1(
    kafka_cluster: ClickHouseCluster,
    instance: ClickHouseInstance,
    create_query_generator,
):
    topic_name = "pretty_space" + get_topic_postfix(create_query_generator)

    with kafka_topic(get_admin_client(kafka_cluster), topic_name):
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
        messages = kafka_consume_with_retry(kafka_cluster, topic_name, message_count)
        assert len(messages) == 3

        data = []
        for message in messages:
            splitted = message.split("\n")
            assert splitted[0] == " \x1b[1mkey\x1b[0m   \x1b[1mvalue\x1b[0m"
            assert splitted[1] == ""
            assert splitted[-1] == ""
            data += [line.split() for line in splitted[2:-1]]

        assert data == [
            ["0", "0"],
            ["10", "100"],
            ["20", "200"],
            ["30", "300"],
            ["40", "400"],
        ]


if __name__ == "__main__":
    init_cluster_and_instance()
    conftest_cluster.start()
    input("Cluster created, press any key to destroy...")
    conftest_cluster.shutdown()
