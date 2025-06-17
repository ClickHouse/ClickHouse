#!/usr/bin/env python3
import os
import sys
import time
import logging
import pytest
import json
from google.protobuf.internal.encoder import _VarintBytes
import base64
import random

from . import sqs_pb2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers.cluster import ClickHouseCluster, ClickHouseInstance

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

localstack_url = "http://localstack:4566/"

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    'instance',
    main_configs=['configs/sqs.xml'],
    with_localstack=True,
    # stay_alive=True,
)

def check_clickhouse_alive(instance):
    """Check if ClickHouse is running and accepting connections"""
    for _ in range(30):
        try:
            # Check using HTTP interface instead of nc
            is_port_open = instance.exec_in_container(
                ["bash", "-c", "curl -s http://localhost:8123/ -d 'SELECT 1' || echo 'Connection failed'"],
                nothrow=True,
                user="root"
            )
            logger.info(f"Check HTTP interface: {is_port_open}")
            
            if "Connection failed" not in is_port_open:
                # If HTTP request is successful, try to execute the query
                try:
                    result = instance.query("SELECT 1")
                    if result.strip() == "1":
                        return True
                except Exception as e:
                    logger.error(f"Error executing query: {e}")
            time.sleep(2)
        except Exception as e:
            logger.error(f"Error checking status: {e}")
            time.sleep(2)
    return False

# Fixture for starting/stopping the cluster
@pytest.fixture(scope="module")
def started_cluster():
    try:
        # Make a minimal test for functionality
        cluster.start()
        
        # Check if ClickHouse is running
        if not check_clickhouse_alive(instance):
            logger.error("ClickHouse did not start or is not responding")
            # Get server log for diagnosis
            try:
                full_log = instance.exec_in_container(
                    ["bash", "-c", "cat /var/log/clickhouse-server/clickhouse-server.log"],
                    nothrow=True,
                    user="root"
                )
                error_log = instance.exec_in_container(
                    ["bash", "-c", "cat /var/log/clickhouse-server/clickhouse-server.err.log"],
                    nothrow=True,
                    user="root"
                )
                
                server_status = instance.exec_in_container(
                    ["bash", "-c", "ps aux | grep clickhouse"],
                    nothrow=True,
                    user="root"
                )
                
                logger.info("ClickHouse process status:")
                logger.info(server_status)
                
                logger.info("Server log content:")
                logger.info(full_log)
                
                if error_log:
                    logger.error("Error log content:")
                    logger.error(error_log)
            except Exception as e:
                logger.error(f"Error reading logs: {e}")
            
            raise Exception("ClickHouse did not start")
       
        instance.query("CREATE DATABASE IF NOT EXISTS test")
        
        # Wait for LocalStack to start
        max_retries = 30
        for i in range(max_retries):
            try:
                localstack_check = instance.exec_in_container(
                    ["bash", "-c", "curl -s http://localstack:4566/health"],
                    nothrow=True,
                    user="root"
                )
                if "services" in localstack_check:
                    logger.info(f"LocalStack started: {localstack_check}")
                    break
                logger.info(f"Waiting for LocalStack, attempt {i+1}/{max_retries}")
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error checking LocalStack: {e}")
                time.sleep(1)
        
        logger.info("Cluster successfully started")
        yield cluster
    except Exception as e:
        # Выведем подробный лог при ошибке
        logger.error(f"Error starting cluster: {e}")
        try:
            docker_logs = instance.exec_in_container(
                ["bash", "-c", "cat /var/log/clickhouse-server/clickhouse-server.log"],
                nothrow=True,
                user="root"
            )
            logger.error(f"ClickHouse log: {docker_logs}")
            
            docker_error_logs = instance.exec_in_container(
                ["bash", "-c", "cat /var/log/clickhouse-server/clickhouse-server.err.log"],
                nothrow=True,
                user="root"
            )
            logger.error(f"ClickHouse error log: {docker_error_logs}")
        except Exception as log_error:
            logger.error(f"Error getting logs: {log_error}")
        raise
    finally:
        cluster.shutdown()
        logger.info("Cluster stopped")

def create_queue(instance, queue_name, visibility_timeout=None):
    if visibility_timeout:
        create_response = instance.exec_in_container(
            ["bash", "-c", f"curl -s -X POST '{localstack_url}?' -H 'Content-Type: application/x-www-form-urlencoded' --data-urlencode 'Action=CreateQueue' --data-urlencode 'QueueName={queue_name}' --data-urlencode 'VisibilityTimeout={visibility_timeout}'"],
            user="root"
        )
    else:
        create_response = instance.exec_in_container(
            ["bash", "-c", f"curl -s -X POST '{localstack_url}?' -H 'Content-Type: application/x-www-form-urlencoded' --data-urlencode 'Action=CreateQueue' --data-urlencode 'QueueName={queue_name}'"],
            user="root"
        )
    logger.info(f"Queue created: {queue_name}, response: {create_response}")

def send_message(instance, queue_name, message):
    send_response = instance.exec_in_container(
        ["bash", "-c", f"curl -s -X POST '{localstack_url}000000000000/{queue_name}' -H 'Content-Type: application/x-www-form-urlencoded' --data-urlencode 'Action=SendMessage' --data-urlencode 'MessageBody={message}'"],
        user="root"
    )
    logger.info(f"Message sent to queue: {queue_name}, response: {send_response}")

# Helper function to check results
def sqs_check_result(result, reference):
    result_lines = sorted([line for line in result.strip().split('\n') if line])
    reference_lines = sorted([line for line in reference.strip().split('\n') if line])
    logger.info(f"result_lines: {result_lines}")
    logger.info(f"reference_lines: {reference_lines}")
    return result_lines == reference_lines

def test_sqs_external_messages(started_cluster):
    """
    Test processing messages that were sent to SQS from an external system:
    1. Create SQS queue
    2. Create SQS table for reading from queue and target table for results
    3. Configure materialized view for automatic processing
    4. Send messages directly to queue via HTTP API
    5. Check that messages ended up in the target table
    """
    logger.info("Starting test of processing external messages from SQS")
    
    # Create a new queue for the test
    queue_name = "external_messages_test"
    create_queue(instance, queue_name)
    
    # Send test messages directly to queue via HTTP API
    # Use the correct API call SendMessage
    for i in range(1, 6):
        message = {"id": i, "message": f"external_message_{i}"}
        message_json = json.dumps(message)
        
        send_message(instance, queue_name, message_json)
    
    # Drop tables
    instance.query('DROP TABLE IF EXISTS test.sqs_target')
    instance.query('DROP TABLE IF EXISTS test.sqs_source')
    instance.query('DROP TABLE IF EXISTS test.sqs_mv')
    
    # Create a target table to store results
    instance.query('''
        CREATE TABLE test.sqs_target (
            id UInt64,
            message String,
            timestamp DateTime64(3) DEFAULT now64()
        ) ENGINE = Memory
    ''')
    
    # Create a SQS table for reading messages
    instance.query(f'''
        CREATE TABLE test.sqs_source (
            id UInt64,
            message String
        ) ENGINE = SQS
        SETTINGS
            sqs_queue_url = 'http://localstack:4566/000000000000/{queue_name}',
            format_name = 'JSONEachRow',
            sqs_auto_delete = 1
    ''')
    
    # Create a materialized view for automatic processing
    instance.query('''
        CREATE MATERIALIZED VIEW test.sqs_mv
        TO test.sqs_target
        AS SELECT id, message FROM test.sqs_source
    ''')
    logger.info("Materialized view created for automatic processing of messages")
    
    time.sleep(3)

    # Wait for the materialized view to process messages
    # Check a few times with small intervals
    max_retries = 10
    for attempt in range(max_retries):
        result = instance.query('SELECT count() FROM test.sqs_target')
        count = int(result.strip())
        logger.info(f"Attempt {attempt+1}/{max_retries}: {count} records in the target table")
        
        if count == 5:  # Expect 5 messages
            break
        
        result = instance.query('SELECT id, message FROM test.sqs_target ORDER BY id')
        logger.info(f"Content of the target table:\n{result}")
            
        time.sleep(2)  # Give time for processing
    
    # Check results
    result = instance.query('SELECT count() FROM test.sqs_target')
    count = int(result.strip())
    assert count == 5, f"Expected 5 messages in the target table, got {count}"
    
    # Check the content of messages
    result = instance.query('SELECT id, message FROM test.sqs_target ORDER BY id')
    logger.info(f"Content of the target table:\n{result}")
    
    # Проверяем, что все сообщения были корректно обработаны
    for i in range(1, 6):
        expected_message = f"external_message_{i}"
        assert expected_message in result, f"Message '{expected_message}' not found in results"
    
    # Cleanup
    instance.query('DROP TABLE IF EXISTS test.sqs_source')
    instance.query('DROP TABLE IF EXISTS test.sqs_target')
    instance.query('DROP TABLE IF EXISTS test.sqs_mv')
    
    logger.info("Test of processing external messages from SQS successfully completed")

def test_sqs_basic(started_cluster):
    """
    Test basic functionality of SQS:
    1. Create SQS table
    2. Write data through INSERT
    3. Read data through SELECT and check results
    """
    logger.info("Starting test of basic functionality of SQS")
    
    create_queue(instance, "default")
    
    # Drop tables
    instance.query('DROP TABLE IF EXISTS test.sqs')
    instance.query('DROP TABLE IF EXISTS test.sqs_data')
    
    # Create a SQS table with the SQS engine
    instance.query('''
        CREATE TABLE test.sqs (
            key UInt64,
            value String
        ) ENGINE = SQS
        SETTINGS
            sqs_queue_url = 'http://localstack:4566/000000000000/default',
            format_name = 'JSONEachRow',
            sqs_max_messages_per_receive = 10,
            sqs_auto_delete = 1,
            num_consumers = 1
    ''')
    
    logger.info("Таблица test.sqs создана")
    
    # Insert data
    instance.query("INSERT INTO test.sqs VALUES (1, 'test_1'), (2, 'test_2'), (3, 'test_3')")
    logger.info("Data inserted into the SQS table")
    
    # Create a memory table for reading data
    instance.query('''
        CREATE TABLE test.sqs_data (
            key UInt64,
            value String
        ) ENGINE = Memory
    ''')
    
    # Wait for messages to be processed
    time.sleep(1)
    
    # Read from SQS and write to memory table
    instance.query('''
        INSERT INTO test.sqs_data
        SELECT * FROM test.sqs
    ''')
    
    # Check the result
    expected = "1\ttest_1\n2\ttest_2\n3\ttest_3\n"
    result = instance.query('SELECT * FROM test.sqs_data ORDER BY key')
    
    assert sqs_check_result(result, expected), \
        f"Expected result: {expected}, Received result: {result}"

    # Cleanup
    instance.query('DROP TABLE IF EXISTS test.sqs')
    instance.query('DROP TABLE IF EXISTS test.sqs_data')
    
    logger.info("Basic SQS test successfully completed")

def test_sqs_materialized_view(started_cluster):
    """
    Test integration of SQS with materialized view:
    1. Create SQS table
    2. Create a materialized view that reads from SQS
    3. Write data to SQS
    4. Check that data ended up in the target table through MV
    """
    logger.info("Starting test of materialized view with SQS")
    
    create_queue(instance, "mv_test")
    
    # Drop tables
    instance.query('DROP TABLE IF EXISTS test.sqs')
    instance.query('DROP TABLE IF EXISTS test.sqs_data')
    instance.query('DROP TABLE IF EXISTS test.sqs_mv')
    
    # Create the target table
    instance.query('''
        CREATE TABLE test.sqs_data (
            key UInt64,
            value String
        ) ENGINE = MergeTree()
        ORDER BY key
    ''')
    
    # Create the SQS table
    instance.query('''
        CREATE TABLE test.sqs (
            key UInt64,
            value String
        ) ENGINE = SQS
        SETTINGS
            sqs_queue_url = 'http://localstack:4566/000000000000/mv_test',
            format_name = 'JSONEachRow',
            sqs_max_messages_per_receive = 10,
            sqs_auto_delete = 1
    ''')
    
    # Insert data into the source table
    instance.query("INSERT INTO test.sqs VALUES (1, 'mv_1'), (2, 'mv_2'), (3, 'mv_3')")
    logger.info("Data inserted into the SQS table")
    
    # Create a materialized view
    instance.query('''
        CREATE MATERIALIZED VIEW test.sqs_mv
        TO test.sqs_data
        AS SELECT * FROM test.sqs
    ''')
    
    logger.info("Materialized view created")
    
    # Wait for data to be processed and end up in the target table
    max_retries = 10
    expected = "1\tmv_1\n2\tmv_2\n3\tmv_3\n"
    result = ""
    
    for i in range(max_retries):
        result = instance.query('SELECT * FROM test.sqs_data ORDER BY key')
        if sqs_check_result(result, expected):
            break
        time.sleep(1)  # Pause between attempts
    
    assert sqs_check_result(result, expected), \
        f"Expected result: {expected}, Received result: {result}"
        
    # Cleanup
    instance.query('DROP TABLE IF EXISTS test.sqs')
    instance.query('DROP TABLE IF EXISTS test.sqs_data')
    instance.query('DROP TABLE IF EXISTS test.sqs_mv')
    
    logger.info("Test of materialized view with SQS successfully completed")

def test_sqs_different_formats(started_cluster):
    """
    Test support of different formats in SQS
    """
    logger.info("Starting test of support of different formats in SQS")
    
    class TestCase:
        def __init__(self, format, table_scheme, send_message_queries, expected):
            self.format = format
            self.queue_name = f"{format}_test"
            self.table_scheme = table_scheme
            self.send_message_queries = send_message_queries
            self.expected = expected
    
    test_cases = [
        TestCase(
            format='CSV',
            table_scheme='key UInt64,\nvalue String',
            send_message_queries=['1,csv_1', '2,csv_2', '3,csv_3'],
            expected='1\tcsv_1\n2\tcsv_2\n3\tcsv_3\n'
        ),
        TestCase(
            format='TSV',
            table_scheme='key UInt64,\nvalue String',
            send_message_queries=['1\ttsv_1', '2\ttsv_2', '3\ttsv_3'],
            expected='1\ttsv_1\n2\ttsv_2\n3\ttsv_3\n'
        ),
        TestCase(
            format='JSONEachRow',
            table_scheme='key UInt64,\nvalue String',
            send_message_queries=['{"key": 1, "value": "json_1"}', '{"key": 2, "value": "json_2"}', '{"key": 3, "value": "json_3"}'],
            expected='1\tjson_1\n2\tjson_2\n3\tjson_3\n'
        ),
        TestCase(
            format='JSONCompactEachRow',
            table_scheme='key UInt64,\nvalue String',
            send_message_queries=['[1, "compact_1"]', '[2, "compact_2"]', '[3, "compact_3"]'],
            expected='1\tcompact_1\n2\tcompact_2\n3\tcompact_3\n'
        ),
        TestCase(
            format='CSVWithNames',
            table_scheme='key UInt64,\nvalue String',
            send_message_queries=['key,value\n1,csv_with_names_1', 'key,value\n2,csv_with_names_2'],
            expected='1\tcsv_with_names_1\n2\tcsv_with_names_2\n'
        ),
        TestCase(
            format='TabSeparated',
            table_scheme='key UInt64,\nvalue String',
            send_message_queries=['1\ttab_separated_1', '2\ttab_separated_2'],
            expected='1\ttab_separated_1\n2\ttab_separated_2\n'
        ),
        TestCase(
            format='TabSeparatedWithNames',
            table_scheme='key UInt64,\nvalue String',
            send_message_queries=['key\tvalue\n1\ttab_with_names_1', 'key\tvalue\n2\ttab_with_names_2'],
            expected='1\ttab_with_names_1\n2\ttab_with_names_2\n'
        ),
    ]
    
    for test_case in test_cases:        
        queue_name = test_case.queue_name
        create_queue(instance, queue_name)
        
        # Drop tables
        instance.query('DROP TABLE IF EXISTS test.sqs')
        instance.query('DROP TABLE IF EXISTS test.sqs_data')
        
        query = f'''
            CREATE TABLE test.sqs ({test_case.table_scheme}) ENGINE = SQS
            SETTINGS
                sqs_queue_url = 'http://localstack:4566/000000000000/{queue_name}',
                format_name = '{test_case.format}',
                sqs_max_messages_per_receive = 10,
                sqs_auto_delete = 1
        '''
        logger.info(f"Creating table, query: {query}")
        instance.query(query)
        
        for send_message_query in test_case.send_message_queries:
            send_message(instance, queue_name, send_message_query)
        
        instance.query(f'''
            CREATE TABLE test.sqs_data ({test_case.table_scheme}) ENGINE = Memory
        ''')
        
        instance.query('''
            INSERT INTO test.sqs_data
            SELECT * FROM test.sqs
        ''')
        
        result = instance.query('SELECT * FROM test.sqs_data ORDER BY key')
    
        assert sqs_check_result(result, test_case.expected), \
            f"Expected result: {test_case.expected}, Received result: {result}"
        
        # Cleanup
        instance.query('DROP TABLE IF EXISTS test.sqs')
        instance.query('DROP TABLE IF EXISTS test.sqs_data')
    
    logger.info("Test of support of different formats in SQS successfully completed")

def test_sqs_concurrent_tables(started_cluster):
    """
    Test of creating multiple SQS tables simultaneously:
    1. Create multiple SQS tables simultaneously
    2. Check that all tables work correctly
    """
    logger.info("Starting test of concurrent work with SQS")
    
    queue_names = [f"parallel_test_{i}" for i in range(1, 4)]
    
    for queue_name in queue_names:
        create_queue(instance, queue_name)

    # Create tables simultaneously
    tables = []
    for i in range(1, 4):
        table_name = f"test.sqs_parallel_{i}"
        queue_name = queue_names[i - 1]
        
        # Drop table
        instance.query(f"DROP TABLE IF EXISTS {table_name}")
        
        instance.query(f'''
            CREATE TABLE {table_name} (
                id UInt64,
                message String
            ) ENGINE = SQS
            SETTINGS
                sqs_queue_url = 'http://localstack:4566/000000000000/{queue_name}',
                format_name = 'JSONEachRow',
                sqs_max_messages_per_receive = 2,
                sqs_auto_delete = 1,
                num_consumers = 1
        ''')
        tables.append(table_name)
        logger.info(f"Table {table_name} created")
    
    # Drop table
    instance.query('DROP TABLE IF EXISTS test.sqs_results')
    
    # Create a table for results
    instance.query('''
        CREATE TABLE test.sqs_results (
            id UInt64,
            message String,
            source String
        ) ENGINE = Memory
    ''')
    
    logger.info("Starting data insertion into tables")
    
    messages_per_table = 5
    for i, table in enumerate(tables, 1):
        for j in range(1, messages_per_table + 1):
            instance.query(f"INSERT INTO {table} VALUES ({j}, 'message_{i}_{j}')")
    
    logger.info("Waiting for message processing")
    time.sleep(5)
    
    # Read data from each table multiple times until all messages are received
    total_read_count = 0
    max_attempts = 10  # Maximum number of reading attempts
    
    for i, table in enumerate(tables, 1):
        logger.info(f"Starting reading from table {table}")
        messages_read = 0
        
        for attempt in range(1, max_attempts + 1):
            logger.info(f"Attempt {attempt} of reading from {table}")
            try:
                # Read in small portions to avoid blocking for a long time
                result = instance.query(f'''
                    INSERT INTO test.sqs_results
                    SELECT id, message, '{table}' as source
                    FROM {table}
                    SETTINGS receive_timeout=2, max_execution_time=5
                ''', timeout=10)
                
                # Check how many messages were read
                read_count = int(instance.query(f"SELECT count() FROM test.sqs_results WHERE source = '{table}'").strip())
                new_messages = read_count - messages_read
                
                logger.info(f"Read {new_messages} new messages from {table}, total: {read_count}")
                
                if new_messages == 0:
                    # If no new messages were received, it means all messages have been read
                    if read_count >= messages_per_table:
                        logger.info(f"Successfully read all messages from {table}")
                        break
                    elif attempt == max_attempts:
                        logger.warning(f"Failed to read all messages from {table}: {read_count}/{messages_per_table}")
                        break
                
                messages_read = read_count
                time.sleep(1)  # Pause between attempts
                
            except Exception as e:
                logger.error(f"Error when reading from {table}: {e}")
                break
        
        # Update the total counter of read messages
        total_read_count += messages_read
    
    # Check the total number of results
    result = instance.query('SELECT count() FROM test.sqs_results')
    expected_count = 3 * messages_per_table  # 3 tables * 5 records
    
    logger.info(f"Total number of received messages: {result.strip()}, expected: {expected_count}")
    
    # Output details about the table content in case of problems
    details = instance.query('SELECT source, count() FROM test.sqs_results GROUP BY source')
    logger.info(f"Details by sources:\n{details}")
    
    assert int(result.strip()) == expected_count, \
        f"Expected {expected_count} records, received {result.strip()}"
    
    # Check that the data from each table is correct
    for table in tables:
        count_result = instance.query(f"SELECT count() FROM test.sqs_results WHERE source = '{table}'")
        assert int(count_result.strip()) == messages_per_table, \
            f"Expected {messages_per_table} records from table {table}, received {count_result.strip()}"
    
    # Cleanup
    for table in tables:
        instance.query(f"DROP TABLE IF EXISTS {table}")
    instance.query("DROP TABLE IF EXISTS test.sqs_results")
    
    logger.info("Test of concurrent work with SQS successfully completed")

def test_sqs_virtual_columns(started_cluster):
    """
    Test of virtual columns in SQS:
    1. Create a table with virtual columns
    2. Write data and check the presence of values in virtual columns
    """
    logger.info("Starting test of virtual columns in SQS")
    
    # Create a queue for the test of virtual columns
    queue_name = "virtual_columns_test"
    create_queue(instance, queue_name)
    
    # Drop tables
    instance.query('DROP TABLE IF EXISTS test.sqs')
    instance.query('DROP TABLE IF EXISTS test.sqs_data')
    
    # Create a table with the SQS engine and selection of virtual columns
    instance.query(f'''
        CREATE TABLE test.sqs (
            key UInt64,
            value String,
            _message_id String,
            _receive_count UInt32,
            _sent_timestamp DateTime64(3)
        ) ENGINE = SQS
        SETTINGS
            sqs_queue_url = 'http://localstack:4566/000000000000/{queue_name}',
            format_name = 'JSONEachRow'
    ''')
    
    # Insert data
    instance.query("INSERT INTO test.sqs (key, value) VALUES (1, 'test_vc_1'), (2, 'test_vc_2')")
    
    # Create a table for storing results
    instance.query('''
        CREATE TABLE test.sqs_data (
            key UInt64,
            value String,
            message_id String,
            receive_count UInt32,
            sent_time DateTime64(3)
        ) ENGINE = Memory
    ''')
    
    # Wait for messages to be processed
    time.sleep(1)
    
    # Read from SQS and check virtual columns
    instance.query('''
        INSERT INTO test.sqs_data
        SELECT 
            key, 
            value, 
            _message_id as message_id,
            _receive_count as receive_count,
            _sent_timestamp as sent_time
        FROM test.sqs
    ''')
    
    # Check that virtual columns contain non-empty values
    result = instance.query('SELECT count() FROM test.sqs_data WHERE message_id != \'\' AND receive_count > 0')
    count = int(result.strip())
    
    assert count == 2, f"Expected 2 records with non-empty virtual columns, received {count}"
    
    # Cleanup
    instance.query('DROP TABLE IF EXISTS test.sqs')
    instance.query('DROP TABLE IF EXISTS test.sqs_data')
    
    logger.info("Test of virtual columns in SQS successfully completed")

def test_sqs_dead_letter_queue(started_cluster):
    """
    Test of Dead Letter Queue (DLQ):
    1. Create a main queue and DLQ
    2. Configure redirection to DLQ after processing errors
    3. Check the operation of the DLQ mechanism
    """
    logger.info("Starting test of Dead Letter Queue")
    
    dlq_name = "dlq_test"
    main_queue_name = "main_queue_test"
    
    create_queue(instance, dlq_name)
    create_queue(instance, main_queue_name)
    
    # Drop tables
    instance.query('DROP TABLE IF EXISTS test.sqs')
    instance.query('DROP TABLE IF EXISTS test.sqs_data')
    instance.query('DROP TABLE IF EXISTS test.sqs_dlq')
    instance.query('DROP TABLE IF EXISTS test.dlq_data')
    instance.query('DROP TABLE IF EXISTS test.sqs_mv')
    
    # Create a table with an incorrect schema (to provoke errors)
    instance.query(f'''
        CREATE TABLE test.sqs (
            key UInt64,  -- We will send String, which will cause an error
            value UInt64
        ) ENGINE = SQS
        SETTINGS
            sqs_queue_url = 'http://localstack:4566/000000000000/{main_queue_name}',
            sqs_dead_letter_queue_url = 'http://localstack:4566/000000000000/{dlq_name}',
            format_name = 'JSONEachRow',
            sqs_auto_delete = 1
    ''')
    
    instance.query('''
        CREATE TABLE test.sqs_data (
            key UInt64,
            value String
        ) ENGINE = MergeTree()
        ORDER BY key
    ''')
    
    instance.query('''
        CREATE MATERIALIZED VIEW test.sqs_mv
        TO test.sqs_data
        AS SELECT key, value FROM test.sqs
    ''')
    
    # Insert incorrect data (string instead of number)
    # Use the correct API call
    message = {"key": "x", "value": 42}
    message_json = json.dumps(message)
    
    send_message(instance, main_queue_name, message_json)
    logger.info(f"Sent incorrect message: {message_json}")
    
    # message = {"key": 1, "value": 42}
    # message_json = json.dumps(message)
    # send_message(instance, main_queue_name, message_json)
    # logger.info(f"Sent correct message: {message_json}")
    
    time.sleep(5)
    
    # Create a table for reading from DLQ (with the correct schema)
    instance.query('''
        CREATE TABLE test.sqs_dlq (
            message String
        ) ENGINE = SQS
        SETTINGS
            sqs_queue_url = 'http://localstack:4566/000000000000/dlq_test',
            format_name = 'Raw'
    ''')
    
    # Create a table for storing results from DLQ
    instance.query('''
        CREATE TABLE test.dlq_data (
            message String
        ) ENGINE = Memory
    ''')
    
    # Wait for the message to be in the DLQ
    time.sleep(5)
    
    # Read from DLQ
    instance.query('''
        INSERT INTO test.dlq_data
        SELECT * FROM test.sqs_dlq
    ''')
    
    # Check that the message is in the DLQ
    result = instance.query('SELECT count() FROM test.dlq_data')
    count = int(result.strip())
    
    assert count > 0, "Message did not get into Dead Letter Queue"
    
    # Cleanup
    instance.query('DROP TABLE IF EXISTS test.sqs')
    instance.query('DROP TABLE IF EXISTS test.sqs_data')
    instance.query('DROP TABLE IF EXISTS test.sqs_dlq')
    instance.query('DROP TABLE IF EXISTS test.dlq_data')
    instance.query('DROP TABLE IF EXISTS test.sqs_mv')
    logger.info("Test of Dead Letter Queue successfully completed")

def test_sqs_error_handling(started_cluster):
    """
    Test of error handling:
    1. Check the skipping of incorrect messages
    """
    logger.info("Starting test of error handling")
    
    # Create a queue for the test of error handling
    queue_name = "error_handling_test"
    create_queue(instance, queue_name)
    
    # Send a mix of correct and incorrect messages
    # Use the correct API call
    messages = [
        {"key": 1, "value": "valid1"},
        {"key": "invalid", "value": "invalid1"},
        {"key": 2, "value": "valid2"}
    ]
    
    for message in messages:
        message_json = json.dumps(message)
        send_message(instance, queue_name, message_json)
        logger.info(f"Sent message: {message}")
    
    # Drop tables
    instance.query('DROP TABLE IF EXISTS test.sqs')
    instance.query('DROP TABLE IF EXISTS test.sqs_data')
    
    # Create a table with the setting to skip incorrect messages
    instance.query('''
        CREATE TABLE test.sqs (
            key UInt64,
            value String
        ) ENGINE = SQS
        SETTINGS
            sqs_queue_url = 'http://localstack:4566/000000000000/error_handling_test',
            format_name = 'JSONEachRow',
            skip_invalid_messages = 1,
            sqs_auto_delete = 1
    ''')
    
    # Create a table for results
    instance.query('''
        CREATE TABLE test.sqs_data (
            key UInt64,
            value String
        ) ENGINE = Memory
    ''')
    
    # Wait for the messages to be processed
    time.sleep(1)
    
    # Read correct messages, skipping incorrect ones
    instance.query('''
        INSERT INTO test.sqs_data
        SELECT * FROM test.sqs
    ''')
    
    # Check the results
    result = instance.query('SELECT count() FROM test.sqs_data')
    count = int(result.strip())
    
    assert count == 2, f"Expected 2 correct messages, received {count}"
    
    # Cleanup
    instance.query('DROP TABLE IF EXISTS test.sqs')
    instance.query('DROP TABLE IF EXISTS test.sqs_data')
    
    logger.info("Test of error handling successfully completed")

def test_read_empty_queue(started_cluster):
    """
    Test of reading from an empty queue:
    1. Create a queue
    2. Create a table with the SQS engine
    3. Check that the table is empty
    """
    queue_name = "empty_queue_test"
    create_queue(instance, queue_name)
    
    # Drop tables
    instance.query('DROP TABLE IF EXISTS test.sqs')
    
    instance.query(f'''
        CREATE TABLE test.sqs (
            id UInt64,
            value String
        ) ENGINE = SQS
        SETTINGS
            sqs_queue_url = 'http://localstack:4566/000000000000/{queue_name}',
            format_name = 'JSONEachRow',
            sqs_auto_delete = 1
    ''')
    
    # Read from the table
    result = instance.query('SELECT * FROM test.sqs')
    
    assert result.strip() == "", "Table should be empty"
    
    # Check that the table is empty
    result = instance.query('SELECT count() FROM test.sqs')
    assert int(result.strip()) == 0, "Table should be empty"
    
    # Cleanup
    instance.query('DROP TABLE IF EXISTS test.sqs')
    
    logger.info("Test of reading from an empty queue successfully completed")

def test_visibility_timeout(started_cluster):
    """
    Test of visibility timeout with materialized view:
    1. Create a queue with a short visibility timeout
    2. Send a message to the queue
    3. Create a materialized view to read the message
    4. Check that the message is read and processed by the materialized view
    5. Check that the message is not available during visibility timeout
    """
    
    queue_name = "visibility_timeout_test"
    create_queue(instance, queue_name)
    
    # Drop tables
    instance.query('DROP TABLE IF EXISTS test.sqs')
    
    instance.query(f'''
        CREATE TABLE test.sqs (
            id UInt64,
            value String
        ) ENGINE = SQS
        SETTINGS
            sqs_queue_url = 'http://localstack:4566/000000000000/{queue_name}',
            format_name = 'JSONEachRow',
            sqs_auto_delete = 0,
            sqs_max_messages_per_receive = 1,
            sqs_visibility_timeout = 5,
            sqs_wait_time_seconds = 1
    ''')
    
    send_message(instance, queue_name, '{"id": 1, "value": "test"}')
    
    # First read
    result = instance.query('SELECT * FROM test.sqs')
    assert "test" in result, "Message should be read by the table"
    
    # Second read (should be empty)
    result2 = instance.query('SELECT * FROM test.sqs')
    assert result2.strip() == "", "Message should not be available during visibility timeout"
    
    # Wait for visibility timeout
    time.sleep(6)
    
    # Third read (should be available again)
    result3 = instance.query('SELECT * FROM test.sqs')
    assert "test" in result3, "Message should be available again"
    
    # Cleanup
    instance.query('DROP TABLE IF EXISTS test.sqs')
    
    logger.info("Test of visibility timeout successfully completed")

if __name__ == '__main__':
    # Ability to manually run tests for debugging
    cluster.start()
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    
    try:
        test_sqs_basic(cluster)
        test_sqs_materialized_view(cluster)
        test_sqs_different_formats(cluster)
        test_sqs_concurrent_tables(cluster)
        test_sqs_virtual_columns(cluster)
        test_sqs_dead_letter_queue(cluster)
        test_sqs_error_handling(cluster)
        test_visibility_timeout(cluster)
    finally:
        cluster.shutdown() 
