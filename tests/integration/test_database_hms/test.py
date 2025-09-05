#!/usr/bin/env python3
import time
import pytest
import os
from helpers.cluster import ClickHouseCluster

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=["config.xml"],
            user_configs=["users.xml"],
            stay_alive=True,
            with_hms_catalog=True,
        )

        cluster.start()
        time.sleep(10)

        yield cluster

    finally:
        cluster.shutdown()

def test_hive_catalog_url_parsing(started_cluster):
    node = started_cluster.instances["node1"]
    
    password = os.environ.get('MINIO_PASSWORD', '[HIDDEN]')
    
    test_databases = [
        'test_valid_url', 'test_missing_protocol', 'test_missing_port',
        'test_invalid_port', 'test_port_zero', 'test_port_too_large',
        'test_empty_port', 'test_complex_path'
    ]
    
    for db_name in test_databases:
        node.query(f"DROP DATABASE IF EXISTS {db_name}")
    
    try:
        node.query(f"""
            CREATE DATABASE test_hms_support_check ENGINE = DataLakeCatalog('thrift://hive:9083', 'minio', '{password}') 
            SETTINGS catalog_type = 'hive', 
                     warehouse = 'test_warehouse', 
                     storage_endpoint = 'http://minio:9000/warehouse-hms/data/'
        """)
        node.query("DROP DATABASE IF EXISTS test_hms_support_check")
    except Exception as e:
        if "compiled without USE_HIVE" in str(e) or "compiled without USE_AVRO" in str(e):
            pytest.skip("HMS catalog not available: ClickHouse compiled without required features")
        if "Invalid URL format" in str(e):
            pass

    try:
        node.query(f"""
            CREATE DATABASE test_valid_url ENGINE = DataLakeCatalog('thrift://hive:9083', 'minio', '{password}') 
            SETTINGS catalog_type = 'hive', 
                     warehouse = 'test_warehouse', 
                     storage_endpoint = 'http://minio:9000/warehouse-hms/data/'
        """)
        node.query("DROP DATABASE IF EXISTS test_valid_url")
    except Exception as e:
        if "Invalid URL format" in str(e):
            pytest.fail("Valid URL should not fail URL parsing")

    try:
        node.query(f"""
            CREATE DATABASE test_missing_protocol ENGINE = DataLakeCatalog('thrift:hive:9083', 'minio', '{password}') 
            SETTINGS catalog_type = 'hive', 
                     warehouse = 'test_warehouse', 
                     storage_endpoint = 'http://minio:9000/warehouse-hms/data/'
        """)
        node.query("SHOW TABLES FROM test_missing_protocol")
        pytest.fail("Missing protocol separator should fail")
    except Exception as e:
        error_msg = str(e)
        if "Invalid URL format: missing protocol separator" not in error_msg:
            pytest.fail(f"Expected protocol separator error, got: {error_msg}")
    finally:
        node.query("DROP DATABASE IF EXISTS test_missing_protocol")

    try:
        node.query(f"""
            CREATE DATABASE test_missing_port ENGINE = DataLakeCatalog('thrift://hive-metastore', 'minio', '{password}') 
            SETTINGS catalog_type = 'hive', 
                     warehouse = 'test_warehouse', 
                     storage_endpoint = 'http://minio:9000/warehouse-hms/data/'
        """)
        node.query("SHOW TABLES FROM test_missing_port")
        pytest.fail("Missing port should fail")
    except Exception as e:
        error_msg = str(e)
        if "Invalid URL format: missing port number" not in error_msg:
            pytest.fail(f"Expected missing port error, got: {error_msg}")
    finally:
        node.query("DROP DATABASE IF EXISTS test_missing_port")

    try:
        node.query(f"""
            CREATE DATABASE test_invalid_port ENGINE = DataLakeCatalog('thrift://hive-metastore:abc', 'minio', '{password}') 
            SETTINGS catalog_type = 'hive', 
                     warehouse = 'test_warehouse', 
                     storage_endpoint = 'http://minio:9000/warehouse-hms/data/'
        """)
        node.query("SHOW TABLES FROM test_invalid_port")
        pytest.fail("Invalid port should fail")
    except Exception as e:
        error_msg = str(e)
        if "Invalid port number: 'abc'" not in error_msg:
            pytest.fail(f"Expected invalid port error, got: {error_msg}")
    finally:
        node.query("DROP DATABASE IF EXISTS test_invalid_port")

    try:
        node.query(f"""
            CREATE DATABASE test_port_zero ENGINE = DataLakeCatalog('thrift://hive-metastore:0', 'minio', '{password}') 
            SETTINGS catalog_type = 'hive', 
                     warehouse = 'test_warehouse', 
                     storage_endpoint = 'http://minio:9000/warehouse-hms/data/'
        """)
        node.query("SHOW TABLES FROM test_port_zero")
        pytest.fail("Port zero should fail")
    except Exception as e:
        error_msg = str(e)
        if "Port number out of valid range (1-65535): 0" not in error_msg:
            pytest.fail(f"Expected port range error, got: {error_msg}")
    finally:
        node.query("DROP DATABASE IF EXISTS test_port_zero")

    try:
        node.query(f"""
            CREATE DATABASE test_port_too_large ENGINE = DataLakeCatalog('thrift://hive-metastore:70000', 'minio', '{password}') 
            SETTINGS catalog_type = 'hive', 
                     warehouse = 'test_warehouse', 
                     storage_endpoint = 'http://minio:9000/warehouse-hms/data/'
        """)
        node.query("SHOW TABLES FROM test_port_too_large")
        pytest.fail("Port too large should fail")
    except Exception as e:
        error_msg = str(e)
        if "Port number out of valid range (1-65535): 70000" not in error_msg:
            pytest.fail(f"Expected port range error, got: {error_msg}")
    finally:
        node.query("DROP DATABASE IF EXISTS test_port_too_large")

    try:
        node.query(f"""
            CREATE DATABASE test_empty_port ENGINE = DataLakeCatalog('thrift://hive-metastore:', 'minio', '{password}') 
            SETTINGS catalog_type = 'hive', 
                     warehouse = 'test_warehouse', 
                     storage_endpoint = 'http://minio:9000/warehouse-hms/data/'
        """)
        node.query("SHOW TABLES FROM test_empty_port")
        pytest.fail("Empty port should fail")
    except Exception as e:
        error_msg = str(e)
        if "Invalid port number: ''" not in error_msg:
            pytest.fail(f"Expected empty port error, got: {error_msg}")
    finally:
        node.query("DROP DATABASE IF EXISTS test_empty_port")

    try:
        node.query(f"""
            CREATE DATABASE test_complex_path ENGINE = DataLakeCatalog('thrift://hive-metastore:9083/metastore/db/table', 'minio', '{password}') 
            SETTINGS catalog_type = 'hive', 
                     warehouse = 'test_warehouse', 
                     storage_endpoint = 'http://minio:9000/warehouse-hms/data/'
        """)
        node.query("DROP DATABASE IF EXISTS test_complex_path")
    except Exception as e:
        if "Invalid URL format" in str(e):
            pytest.fail("Complex path URL should not fail URL parsing")
    finally:
        node.query("DROP DATABASE IF EXISTS test_complex_path")
