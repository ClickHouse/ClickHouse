import pytest
from helpers.cluster import ClickHouseCluster
from pathlib import Path
from textwrap import dedent

cluster = ClickHouseCluster(__file__, name="dict")
node = cluster.add_instance(
    'node',
    with_foundationdb=True,
    stay_alive=True
)

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start(destroy_dirs=True)

        yield cluster

    finally:
        cluster.shutdown()

def test_dict_basic_ddl():
    dict_name = "test_dict_basic_ddl"
    node.query(dedent(f"""\
        CREATE DICTIONARY {dict_name} 
        (   
            `id` UInt64,
            `value` UInt64 DEFAULT 0
        ) 
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dictionary_source_table')) 
        LAYOUT(DIRECT())
    """) )
    assert node.query(f"SELECT count() FROM system.dictionaries WHERE name = '{dict_name}'").strip() == "1"

    node.query(f"DETACH DICTIONARY {dict_name}")
    assert node.query(f"SELECT count() FROM system.dictionaries WHERE name = '{dict_name}'").strip() == "0"
    node.query(f"ATTACH DICTIONARY {dict_name}")
    assert node.query(f"SELECT count() FROM system.dictionaries WHERE name = '{dict_name}'").strip() == "1"
    node.query(f"RENAME DICTIONARY {dict_name} TO {dict_name}2")
    assert node.query(f"SELECT count() FROM system.dictionaries WHERE name = '{dict_name}'").strip() == "0"
    assert node.query(f"SELECT count() FROM system.dictionaries WHERE name = '{dict_name}2'").strip() == "1"
    node.query(f"DROP DICTIONARY {dict_name}2")
    assert node.query(f"SELECT count() FROM system.dictionaries WHERE name = '{dict_name}2'").strip() == "0"
