#!/usr/bin/env python3
import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers.cluster import ClickHouseCluster, ClickHouseInstance

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
    main_configs=['configs/kinesis.xml'],
    with_minio=True, # Using minio instead of real AWS for tests
)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_kinesis_basic(started_cluster):
    instance.query('''
        CREATE TABLE test_kinesis (
            key UInt64,
            value String
        ) ENGINE = Kinesis
        SETTINGS
            kinesis_stream_name = 'clickhouse-test-stream',
            kinesis_format = 'JSONEachRow',
            kinesis_max_records_per_request = 10
    ''')

    # First insert data through clickhouse
    instance.query("INSERT INTO test_kinesis VALUES (1, 'test_1'), (2, 'test_2'), (3, 'test_3')")

    # Create a table for receiving data
    instance.query('''
        CREATE TABLE test_kinesis_data (
            key UInt64,
            value String
        ) ENGINE = Memory
    ''')

    # Get data from Kinesis and insert into Memory table
    instance.query('''
        INSERT INTO test_kinesis_data
        SELECT * FROM test_kinesis
    ''')

    # Verify that the data was successfully received
    result = instance.query('SELECT * FROM test_kinesis_data ORDER BY key')
    assert result == '1\ttest_1\n2\ttest_2\n3\ttest_3\n'

    # Clean up tables
    instance.query('DROP TABLE test_kinesis')
    instance.query('DROP TABLE test_kinesis_data')

if __name__ == '__main__':
    cluster.start()
    input("Cluster started, press any key to shutdown")
    cluster.shutdown() 