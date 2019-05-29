import pytest
import os

from helpers.cluster import ClickHouseCluster
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__, base_configs_dir=os.path.join(SCRIPT_DIR, 'configs'))
node1 = cluster.add_instance('node1', main_configs=[os.path.join(SCRIPT_DIR, 'configs/dictionaries/simple_dictionary.xml')])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node1.query("CREATE DATABASE IF NOT EXISTS test")
        node1.query("CREATE TABLE test.dict_table (id UInt64, SomeField String) ENGINE=MergeTree() ORDER BY tuple()")
        yield cluster
    except Exception as ex:
        print ex
    finally:
        cluster.shutdown()

def test_dictionary_ddl(started_cluster):
    node1.query("INSERT INTO test.dict_table VALUES(1, 'Hello')")
    node1.query("SYSTEM RELOAD DICTIONARIES")

    assert node1.query("select dictGetString('xml_dict', 'SomeField', toUInt64(1))") == "Hello\n"
    assert node1.query("select dictGetString('xml_dict', 'SomeField', toUInt64(2))") == "empty\n"

    node1.query("""
        CREATE DICTIONARY test.ddl_dict(SomeField String DEFAULT 'empty')
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(host 'localhost', port 9000, user 'default', password '', db 'test', table 'dict_table'))
        LAYOUT(FLAT())
        LIFETIME(MIN 10, MAX 60);
    """)

    node1.query("SYSTEM RELOAD DICTIONARIES")

    assert node1.query("select dictGetString('test.ddl_dict', 'SomeField', toUInt64(1))") == "Hello\n"
    assert node1.query("select dictGetString('test.ddl_dict', 'SomeField', toUInt64(2))") == "\\'empty\\'\n"
