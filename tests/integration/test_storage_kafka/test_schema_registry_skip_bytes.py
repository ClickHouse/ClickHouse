"""Tests for kafka_schema_registry_skip_bytes setting"""

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.kafka.common_direct import *
import helpers.kafka.common as k


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


def test_schema_registry_skip_bytes_validation(kafka_cluster):
    """Test validation of kafka_schema_registry_skip_bytes values at table creation time"""
    topic_name = f"skip_bytes_validation_{k.random_string(6)}"
    
    # Test valid values: 0, 19 (AWS Glue), and 255 (max) - these should succeed
    valid_values = [0, 19, 255]
    for skip_bytes in valid_values:
        table_name = f"kafka_table_valid_{skip_bytes}_{k.random_string(4)}"
        create_query = k.generate_new_create_table_query(
            table_name,
            "key String, value String",
            topic_list=topic_name,
            consumer_group=f"{topic_name}_group_{skip_bytes}",
            settings={"kafka_schema_registry_skip_bytes": skip_bytes}
        )
        # This should succeed without errors
        instance.query(create_query)
        instance.query(f"DROP TABLE IF EXISTS test.{table_name}")
    
    # Test invalid values: 256 and above should be rejected at table creation
    invalid_values = [256, 300, 1000, 65536]
    for skip_bytes in invalid_values:
        table_name = f"kafka_table_invalid_{skip_bytes}_{k.random_string(4)}"
        create_query = k.generate_new_create_table_query(
            table_name,
            "key String, value String", 
            topic_list=topic_name,
            consumer_group=f"{topic_name}_group_{skip_bytes}",
            settings={"kafka_schema_registry_skip_bytes": skip_bytes}
        )
        # This should fail with BAD_ARGUMENTS error at table creation time
        with pytest.raises(Exception) as exc_info:
            instance.query(create_query)
        # Verify the error message mentions the limit
        error_message = str(exc_info.value)
        assert "must be between 0 and" in error_message or "BAD_ARGUMENTS" in error_message


def test_schema_registry_skip_bytes_message_processing(kafka_cluster):
    """Test that bytes are actually skipped when processing messages"""
    topic_name = f"skip_bytes_processing_{k.random_string(6)}"
    table_name = f"kafka_skip_test_{k.random_string(6)}"
    
    # Use skip_bytes = 19 for this test (simulating AWS Glue envelope header)
    skip_bytes = 19
    
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
        
        # Create a binary message with 19-byte envelope header + JSON payload
        # Simulate AWS Glue envelope: 19-byte header + valid JSON
        envelope_header = b'\x00' * 19  # 19 bytes envelope header (AWS Glue format)
        json_payload = b'{"message": "test_value"}'  # Valid JSON payload
        message_with_envelope = envelope_header + json_payload
        
        # Produce the binary message using raw producer
        from kafka import KafkaProducer
        
        producer = KafkaProducer(
            bootstrap_servers=f"localhost:{kafka_cluster.kafka_port}",
            value_serializer=lambda x: x  # No serialization, send raw bytes
        )
        
        producer.send(topic_name, value=message_with_envelope)
        producer.flush()
        producer.close()
        
        # Create materialized view to consume messages
        mv_table = f"mv_{table_name}"
        instance.query(f"""
            CREATE TABLE test.{mv_table} (
                message String
            ) ENGINE = MergeTree()
            ORDER BY message
        """)
        
        instance.query(f"""
            CREATE MATERIALIZED VIEW test.{table_name}_mv TO test.{mv_table} AS
            SELECT message FROM test.{table_name}
        """)
        
        # Wait for message to be processed and check the exact result
        result = instance.query_with_retry(
            f"SELECT message FROM test.{mv_table}",
            check_callback=lambda res: res.strip() == "test_value",
            retry_count=30,
            sleep_time=1
        )
        
        # Verify the exact result - should be exactly "test_value" after skipping envelope
        assert result.strip() == "test_value", f"Expected 'test_value', got '{result.strip()}'"
        
        # Clean up
        instance.query(f"DROP VIEW IF EXISTS test.{table_name}_mv")
        instance.query(f"DROP TABLE IF EXISTS test.{mv_table}")
        instance.query(f"DROP TABLE IF EXISTS test.{table_name}")
