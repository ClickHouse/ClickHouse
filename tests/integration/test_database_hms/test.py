#!/usr/bin/env python3
import time
import pytest
import os
import socket
import uuid

import pyarrow as pa
from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key, minio_access_key
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
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
                     storage_endpoint = 'http://minio1:9001/warehouse-hms/data/'
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
                     storage_endpoint = 'http://minio1:9001/warehouse-hms/data/'
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
                     storage_endpoint = 'http://minio1:9001/warehouse-hms/data/'
        """)
        node.query("select * FROM test_missing_protocol.abc")
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
                     storage_endpoint = 'http://minio1:9001/warehouse-hms/data/'
        """)
        node.query("select * FROM test_missing_port.abc")
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
                     storage_endpoint = 'http://minio1:9001/warehouse-hms/data/'
        """)
        node.query("select * FROM test_invalid_port.abc")
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
                     storage_endpoint = 'http://minio1:9001/warehouse-hms/data/'
        """)
        node.query("select * FROM test_port_zero.abc")
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
                     storage_endpoint = 'http://minio1:9001/warehouse-hms/data/'
        """)
        node.query("select * FROM test_port_too_large.abc")
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
                     storage_endpoint = 'http://minio1:9001/warehouse-hms/data/'
        """)
        node.query("select * FROM test_empty_port.abc")
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
                     storage_endpoint = 'http://minio1:9001/warehouse-hms/data/'
        """)
        node.query("DROP DATABASE IF EXISTS test_complex_path")
    except Exception as e:
        if "Invalid URL format" in str(e):
            pytest.fail("Complex path URL should not fail URL parsing")
    finally:
        node.query("DROP DATABASE IF EXISTS test_complex_path")


def test_check_database(started_cluster):
    node = started_cluster.instances["node1"]

    password = os.environ.get('MINIO_PASSWORD', '[HIDDEN]')

    node.query("DROP DATABASE IF EXISTS test_hms_check_db")

    try:
        node.query(f"""
            CREATE DATABASE test_hms_check_db ENGINE = DataLakeCatalog('thrift://hive:9083', 'minio', '{password}') 
            SETTINGS catalog_type = 'hive', 
                     warehouse = 'test_warehouse', 
                     storage_endpoint = 'http://minio1:9001/warehouse-hms/data/'
        """)
        node.query("CHECK DATABASE test_hms_check_db")

        node.query(
            "SYSTEM ENABLE FAILPOINT check_database_datalake_negative"
        )

        assert "fault when checking database" in node.query_and_get_error(
            "CHECK DATABASE test_hms_check_db"
        )
    except Exception as e:
        if "compiled without USE_HIVE" in str(e) or "compiled without USE_AVRO" in str(e):
            pytest.skip("HMS catalog not available: ClickHouse compiled without required features")
        if "Invalid URL format" in str(e):
            pass
    finally:
        node.query("SYSTEM DISABLE FAILPOINT check_database_datalake_negative")
        node.query("DROP DATABASE IF EXISTS test_hms_check_db")


def wait_for_hms(started_cluster, timeout=120):
    hive_ip = started_cluster.get_instance_ip("hive")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((hive_ip, 9083), timeout=2):
                return
        except OSError:
            time.sleep(1)
    raise Exception(f"Hive Metastore at {hive_ip}:9083 did not become available within {timeout}s")


def test_query_condition_cache(started_cluster):
    node = started_cluster.instances["node1"]

    wait_for_hms(started_cluster)

    catalog = load_catalog(
        "hive",
        **{
            "uri": f"thrift://{started_cluster.get_instance_ip('hive')}:9083",
            "type": "hive",
            "s3.endpoint": f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )
    namespace = f"qcc_{uuid.uuid4().hex[:8]}"
    catalog.create_namespace(namespace)
    table = catalog.create_table(
        identifier=f"{namespace}.t",
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        ),
        location=f"s3a://warehouse-hms/data/{namespace}/t",
        properties={"write.parquet.row-group-limit": "1000"},
    )
    table.append(
        pa.table(
            {
                "id": pa.array(range(3000), type=pa.int64()),
                "name": pa.array([str(i) for i in range(3000)], type=pa.string()),
            }
        )
    )

    node.query(f"""
        DROP DATABASE IF EXISTS hms_qcc;
        CREATE DATABASE hms_qcc ENGINE = DataLakeCatalog('thrift://hive:9083', '{minio_access_key}', '{minio_secret_key}')
        SETTINGS catalog_type = 'hive',
                 warehouse = 'test_warehouse',
                 storage_endpoint = 'http://minio1:9001/warehouse-hms'
    """)

    query = f"SELECT count(), sum(id) FROM hms_qcc.`{namespace}.t` WHERE id BETWEEN 1100 AND 1200"
    query_ids = [f"hms_qcc_{namespace}_{i}" for i in range(2)]
    for query_id in query_ids:
        assert node.query(query, query_id=query_id, settings={"use_query_condition_cache": 1}) == "101\t116150\n"

    node.query("SYSTEM FLUSH LOGS query_log")
    events = [
        node.query(
            f"""
            SELECT ProfileEvents['QueryConditionCacheMisses'], ProfileEvents['QueryConditionCacheHits']
            FROM system.query_log
            WHERE query_id = '{query_id}' AND type = 'QueryFinish'
            """
        ).split()
        for query_id in query_ids
    ]
    assert int(events[0][0]) > 0 and int(events[0][1]) == 0, events
    assert int(events[1][1]) > 0, events

    node.query("DROP DATABASE hms_qcc")
