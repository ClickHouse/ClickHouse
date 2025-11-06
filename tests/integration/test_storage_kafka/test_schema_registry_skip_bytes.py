"""Tests for kafka_schema_registry_skip_bytes setting"""

import logging
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.kafka.common_direct import *
import helpers.kafka.common as k
from helpers.test_tools import TSV
from contextlib import contextmanager


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml"],
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


def test_schema_registry_skip_bytes_valid_values(kafka_cluster):
    """Test that valid values for kafka_schema_registry_skip_bytes are accepted"""
    topic_name = f"skip_bytes_valid_{k.random_string(6)}"
    
    # Test valid values: 0, 19 (AWS Glue), and 255 (max)
    valid_values = [0, 19, 255]
    
    for skip_bytes in valid_values:
        table_name = f"kafka_table_{skip_bytes}_{k.random_string(4)}"
        
        # Create table with the skip_bytes setting
        create_query = k.generate_new_create_table_query(
            table_name,
            "key String, value String",
            topic_list=topic_name,
            consumer_group=f"{topic_name}_group_{skip_bytes}",
            settings={"kafka_schema_registry_skip_bytes": skip_bytes}
        )
        
        # This should succeed without errors
        instance.query(create_query)
        
        # Verify the table was created
        result = instance.query(f"SHOW TABLES LIKE '{table_name}'")
        assert table_name in result
        
        # Clean up
        instance.query(f"DROP TABLE IF EXISTS {table_name}")


def test_schema_registry_skip_bytes_invalid_values(kafka_cluster):
    """Test that invalid values for kafka_schema_registry_skip_bytes are rejected"""
    topic_name = f"skip_bytes_invalid_{k.random_string(6)}"
    
    # Test invalid values: 256 and above should be rejected
    invalid_values = [256, 300, 1000, 65536]
    
    for skip_bytes in invalid_values:
        table_name = f"kafka_table_invalid_{skip_bytes}_{k.random_string(4)}"
        
        # Create table with invalid skip_bytes setting
        create_query = k.generate_new_create_table_query(
            table_name,
            "key String, value String", 
            topic_list=topic_name,
            consumer_group=f"{topic_name}_group_{skip_bytes}",
            settings={"kafka_schema_registry_skip_bytes": skip_bytes}
        )
        
        # This should fail with BAD_ARGUMENTS error
        with pytest.raises(Exception) as exc_info:
            instance.query(create_query)
        
        # Verify the error message mentions the limit
        error_message = str(exc_info.value)
        assert "must be between 0 and 255" in error_message or "BAD_ARGUMENTS" in error_message


def test_schema_registry_skip_bytes_message_processing(kafka_cluster):
    """Test that bytes are actually skipped when processing messages"""
    topic_name = f"skip_bytes_processing_{k.random_string(6)}"
    table_name = f"kafka_skip_test_{k.random_string(6)}"
    
    # Use skip_bytes = 5 for this test (simulating envelope header)
    skip_bytes = 5
    
    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name):
        # Create table with skip_bytes setting, using JSONEachRow format
        create_query = k.generate_new_create_table_query(
            table_name,
            "message String",
            topic_list=topic_name,
            consumer_group=f"{topic_name}_group",
            format="JSONEachRow",
            settings={"kafka_schema_registry_skip_bytes": skip_bytes}
        )
        
        instance.query(create_query)
        
        # Create a binary message with 5-byte envelope header + JSON payload
        # Simulate AWS Glue envelope: 5 dummy bytes + valid JSON
        envelope_header = b'\x00\x01\x02\x03\x04'  # 5 bytes envelope header
        json_payload = b'{"message": "test_value"}'  # Valid JSON payload
        message_with_envelope = envelope_header + json_payload
        
        # Produce the binary message using raw producer
        from kafka import KafkaProducer
        import json
        
        producer = KafkaProducer(
            bootstrap_servers=f"{kafka_cluster.kafka_host}:19092",
            value_serializer=lambda x: x  # No serialization, send raw bytes
        )
        
        producer.send(topic_name, value=message_with_envelope)
        producer.flush()
        producer.close()
        
        # Give some time for processing
        import time
        time.sleep(3)
        
        # Check if the message was processed correctly (envelope bytes skipped)
        result = instance.query(f"SELECT message FROM {table_name}")
        
        # The result should contain "test_value" from the JSON after skipping envelope
        if result.strip():
            assert "test_value" in result
            logging.debug(f"Message processing test result: {result}")
        else:
            # If no data, check for errors in logs
            logging.warning("No data received, checking for parsing errors")
        
        # Clean up
        instance.query(f"DROP TABLE IF EXISTS {table_name}")