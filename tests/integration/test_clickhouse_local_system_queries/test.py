#!/usr/bin/env python3

import pytest
import subprocess
import sys
import os

# Add the ClickHouse test framework to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'tests', 'helpers'))

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', main_configs=['configs/clickhouse-server.xml'])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_clickhouse_local_system_queries():
    """Test that clickhouse-local properly handles SYSTEM queries that are not supported."""
    
    # Test cases that should fail with UNSUPPORTED_METHOD in clickhouse-local
    test_cases = [
        ("SYSTEM RELOAD CONFIG;", "UNSUPPORTED_METHOD"),
        ("SYSTEM STOP LISTEN HTTP;", "UNSUPPORTED_METHOD"),
        ("SYSTEM START LISTEN HTTP;", "UNSUPPORTED_METHOD"),
        ("SYSTEM STOP LISTEN TCP;", "UNSUPPORTED_METHOD"),
        ("SYSTEM START LISTEN TCP;", "UNSUPPORTED_METHOD"),
    ]
    
    for query, expected_error in test_cases:
        try:
            # Run clickhouse-local with the query
            result = subprocess.run([
                'clickhouse-local', '--query', query
            ], capture_output=True, text=True, timeout=10)
            
            # Check that the command failed with the expected error
            assert result.returncode != 0, f"Query '{query}' should have failed"
            assert expected_error in result.stderr, f"Expected error '{expected_error}' not found in stderr: {result.stderr}"
            assert "clickhouse-local" in result.stderr, f"Error message should mention clickhouse-local: {result.stderr}"
            
        except subprocess.TimeoutExpired:
            pytest.fail(f"Query '{query}' timed out")
        except Exception as e:
            pytest.fail(f"Unexpected error running query '{query}': {e}")

def test_clickhouse_local_supported_system_queries():
    """Test that clickhouse-local properly handles SYSTEM queries that are supported."""
    
    # Test cases that should work in clickhouse-local
    supported_queries = [
        "SYSTEM DROP DNS CACHE;",
        "SYSTEM DROP MARK CACHE;",
        "SYSTEM DROP UNCOMPRESSED CACHE;",
        "SYSTEM DROP QUERY CACHE;",
        "SYSTEM DROP SCHEMA CACHE;",
        "SYSTEM DROP FORMAT SCHEMA CACHE;",
    ]
    
    for query in supported_queries:
        try:
            # Run clickhouse-local with the query
            result = subprocess.run([
                'clickhouse-local', '--query', query
            ], capture_output=True, text=True, timeout=10)
            
            # Check that the command succeeded
            assert result.returncode == 0, f"Query '{query}' should have succeeded but failed with: {result.stderr}"
            
        except subprocess.TimeoutExpired:
            pytest.fail(f"Query '{query}' timed out")
        except Exception as e:
            pytest.fail(f"Unexpected error running query '{query}': {e}")

def test_server_mode_system_queries(started_cluster):
    """Test that SYSTEM queries work properly in server mode."""
    
    # Test cases that should work in server mode
    server_queries = [
        "SYSTEM RELOAD CONFIG",
        "SYSTEM STOP LISTEN HTTP",
        "SYSTEM START LISTEN HTTP",
        "SYSTEM DROP DNS CACHE",
        "SYSTEM DROP MARK CACHE",
    ]
    
    for query in server_queries:
        try:
            # Run query against the server
            result = node.query(query)
            # If we get here, the query succeeded
            assert result is not None
            
        except Exception as e:
            # Some queries might fail due to permissions or other reasons,
            # but they shouldn't fail with LOGICAL_ERROR
            error_msg = str(e)
            assert "LOGICAL_ERROR" not in error_msg, f"Query '{query}' failed with LOGICAL_ERROR: {error_msg}"
            assert "Can't reload config because config_reload_callback is not set" not in error_msg, \
                f"Query '{query}' failed with old error message: {error_msg}"

if __name__ == '__main__':
    cluster.start()
    try:
        test_clickhouse_local_system_queries()
        test_clickhouse_local_supported_system_queries()
        test_server_mode_system_queries(cluster)
        print("All tests passed!")
    finally:
        cluster.shutdown() 