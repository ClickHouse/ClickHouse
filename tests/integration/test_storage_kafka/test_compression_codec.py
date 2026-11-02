"""Tests for kafka_compression_codec setting"""

import logging

import pytest

from helpers.cluster import ClickHouseCluster, is_arm
from helpers.kafka.common_direct import *
import helpers.kafka.common as k
from helpers.test_tools import TSV
from contextlib import contextmanager

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml", "configs/compression_codec.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,
    stay_alive=True,
    macros={
        "kafka_broker": "kafka1",
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
    k.clean_test_database_and_topics(instance, cluster)
    yield  # run test


@contextmanager
def restart_clickhouse_after():
    yield
    instance.restart_clickhouse()


def get_applicable_compression_level(compression_codec):
    if compression_codec == "gzip":
        return 7
    elif compression_codec == "snappy":
        return 0
    elif compression_codec == "lz4":
        return 12
    elif compression_codec == "zstd":
        return 11
    elif compression_codec == "none" or compression_codec is None:
        return None

    raise ValueError(f"Unknown compression codec: {compression_codec}")


def assert_producer_property_set_in_logs(instance, table_name, property_name, property_value):
    maybe_uuid_regex = r"( \([a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}\))?"
    instance.wait_for_log_line(fr"StorageKafka2? \(test\.{table_name}{maybe_uuid_regex}\): Producer set property {property_name}:{property_value}")


def assert_compression_codec_in_logs(instance, table_name, compression_codec):
    assert_producer_property_set_in_logs(instance, table_name, r"compression\.codec", compression_codec)


def assert_compression_level_in_logs(instance, table_name, compression_level):
    assert_producer_property_set_in_logs(instance, table_name, r"compression\.level", compression_level)


# Test for different compression codec values
@pytest.mark.parametrize(
    "compression_codec",
    [None, "none", "gzip", "snappy", "lz4", "zstd"],
)
@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_compression_codec(kafka_cluster, compression_codec, create_query_generator):
    # The difference between (the python object) None and (the string) "none":
    #  - None means that the setting is not set at all, thus the default value ("none") will be used
    #  - "none" means that the setting is explicitly set to "none"
    # As a result both cases should lead to the same behavior, i.e. no compression, but for different reasons.
    """Test that kafka_compression_codec setting works correctly for both producing and consuming messages"""
    storage_version = create_query_generator.__name__.replace("generate_", "").replace("_create_table_query", "")
    # It is better to use a random string, because it makes logs more unique and avoids false positive test runs
    suffix = f"{storage_version}_{compression_codec if compression_codec is not None else 'default'}_{k.random_string(6)}"
    kafka_table_producer = f"kafka_producer_{suffix}"
    kafka_table_consumer = f"kafka_consumer_{suffix}"
    topic_name = f"compression_codec_{suffix}"
    compression_level = get_applicable_compression_level(compression_codec)
    table_settings = {}

    if compression_codec is not None:
        table_settings["kafka_compression_codec"] = compression_codec

    if compression_level is not None:
        table_settings["kafka_compression_level"] = compression_level

    admin_client = k.get_admin_client(kafka_cluster)

    with k.kafka_topic(admin_client, topic_name):
        logging.debug(f"Testing compression codec: {compression_codec if compression_codec is not None else 'default'}")

        # Create producer table with compression codec using generator
        producer_create_query = create_query_generator(
            kafka_table_producer,
            "key UInt64, value String",
            topic_list=topic_name,
            consumer_group=f"{topic_name}_producer",
            settings=table_settings
        )
        instance.query(producer_create_query)

        # Create consumer table with compression codec using generator
        consumer_create_query = create_query_generator(
            kafka_table_consumer,
            "key UInt64, value String",
            topic_list=topic_name,
            consumer_group=f"{topic_name}_consumer"
        )
        instance.query(consumer_create_query)

        # Create materialized view to store consumed data
        instance.query(f"""
            CREATE TABLE test.{kafka_table_consumer}_view (key UInt64, value String)
                ENGINE = MergeTree()
                ORDER BY key;
            CREATE MATERIALIZED VIEW test.{kafka_table_consumer}_mv TO test.{kafka_table_consumer}_view AS
                SELECT * FROM test.{kafka_table_consumer};
        """)

        # Insert test data into producer table
        messages_count = 20
        values = []
        expected_data = []
        for i in range(messages_count):
            test_value = f"test_message_{compression_codec}_{i}"
            values.append(f"({i}, '{test_value}')")
            expected_data.append(f"{i}\t{test_value}")

        values_str = ",".join(values)
        instance.query(f"INSERT INTO test.{kafka_table_producer} VALUES {values_str}")

        # Unfortunately there is no way to directly verify the compression codec using the python client
        assert_compression_codec_in_logs(instance, kafka_table_producer, compression_codec if compression_codec is not None else "none")
        if compression_level is not None:
            assert_compression_level_in_logs(instance, kafka_table_producer, compression_level)

        # Wait for messages to be consumed
        expected_result = "\n".join(expected_data)
        result = instance.query_with_retry(
            f"SELECT * FROM test.{kafka_table_consumer}_view ORDER BY key",
            check_callback=lambda res: res.count("\n") == messages_count,
            retry_count=30,
            sleep_time=1
        )

        assert TSV(result) == TSV(expected_result)

        # Clean up
        instance.query(f"DROP TABLE test.{kafka_table_producer}")
        instance.query(f"DROP TABLE test.{kafka_table_consumer}")
        instance.query(f"DROP TABLE test.{kafka_table_consumer}_mv")
        instance.query(f"DROP TABLE test.{kafka_table_consumer}_view")


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_settings_precedence(kafka_cluster, create_query_generator):
    # The precedence of settings in general:
    # 1. Settings in create query (gzip)
    # 2. Settings in producer/consumer specific settings (snappy)
    # 3. Settings in the general kafka settings from config (lz4)
    # It is not optimal (probably the settings from the create query should have absolute precedence), but changing this would be a breaking change,
    # thus it doesn't make sense until there is a very good reason and an actual use case that requires it.
    storage_version = create_query_generator.__name__.replace("generate_", "").replace("_create_table_query", "")
    suffix = f"{storage_version}_{k.random_string(6)}"
    topic_name = f"settings_precedence_{suffix}"
    table_name = f"{topic_name}"

    general_kafka_config = """
    <clickhouse>
    <kafka>
        <compression_codec>lz4</compression_codec>
    </kafka>
</clickhouse>
"""

    producer_specific_config = """
    <clickhouse>
    <kafka>
        <compression_codec>lz4</compression_codec>
        <producer>
            <compression_codec>snappy</compression_codec>
        </producer>
    </kafka>
</clickhouse>
"""
    config_file_path = "/etc/clickhouse-server/config.d/compression_codec.xml"

    # Clickhouse has to be restarted to apply the default config
    with restart_clickhouse_after():
        # The topic doesn't need to be created, because we only check the applied compression codec in producer creation logs
        with instance.with_replace_config(config_file_path, producer_specific_config):
            instance.restart_clickhouse()

            instance.query(create_query_generator(
                table_name,
                "key UInt64, value String",
                topic_list=topic_name,
                consumer_group=f"{topic_name}_consumer",
                settings={"kafka_compression_codec": "gzip"}
            ))

            instance.query(f"INSERT INTO test.{table_name} VALUES (1, 'test_message')")
            # The value from the create query takes precedence over the setting in general kafka section
            assert_compression_codec_in_logs(instance, table_name, "gzip")

            instance.query(f"DROP TABLE test.{table_name} SYNC")

            # Let's create a table without kafka_compression_codec setting in the create query
            instance.query(create_query_generator(
                table_name,
                "key UInt64, value String",
                topic_list=topic_name,
                consumer_group=f"{topic_name}_consumer",
            ))
            instance.query(f"INSERT INTO test.{table_name} VALUES (2, 'test_message 2')")
            assert_compression_codec_in_logs(instance, table_name, "snappy")

        with instance.with_replace_config(config_file_path, general_kafka_config):
            instance.restart_clickhouse()
            instance.query(f"INSERT INTO test.{table_name} VALUES (3, 'test_message 3')")
            assert_compression_codec_in_logs(instance, table_name, "lz4")

        instance.query(f"DROP TABLE test.{table_name} SYNC")


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()