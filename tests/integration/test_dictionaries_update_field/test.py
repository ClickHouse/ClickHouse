## sudo -H pip install PyMySQL
import time

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.cluster import ClickHouseKiller
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance('main_node', main_configs=[])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node.query(
            """
            CREATE TABLE table_for_update_field_dictionary
            (
                key UInt64,
                value String,
                last_insert_time DateTime
            )
            ENGINE = TinyLog;
            """
        )

        yield cluster

    finally:
        cluster.shutdown()

@pytest.mark.parametrize("dictionary_name,dictionary_type", [
    ("flat_update_field_dictionary", "FLAT"),
    ("simple_key_hashed_update_field_dictionary", "HASHED"),
    ("complex_key_hashed_update_field_dictionary", "HASHED")
])
def test_update_field(started_cluster, dictionary_name, dictionary_type):
    create_dictionary_query = """
        CREATE DICTIONARY {dictionary_name}
        (
            key UInt64,
            value String,
            last_insert_time DateTime
        )
        PRIMARY KEY key
        SOURCE(CLICKHOUSE(table 'table_for_update_field_dictionary' update_field 'last_insert_time'))
        LAYOUT({dictionary_type}())
        LIFETIME(1);
        """.format(dictionary_name=dictionary_name, dictionary_type=dictionary_type)

    node.query(create_dictionary_query)

    node.query("INSERT INTO table_for_update_field_dictionary VALUES (1, 'First', now());")
    query_result = node.query("SELECT key, value FROM {dictionary_name} ORDER BY key ASC".format(dictionary_name=dictionary_name))
    assert query_result == '1\tFirst\n'

    node.query("INSERT INTO table_for_update_field_dictionary VALUES (2, 'Second', now());")
    time.sleep(10)

    query_result = node.query("SELECT key, value FROM {dictionary_name} ORDER BY key ASC".format(dictionary_name=dictionary_name))

    assert query_result == '1\tFirst\n2\tSecond\n'

    node.query("INSERT INTO table_for_update_field_dictionary VALUES (2, 'SecondUpdated', now());")
    node.query("INSERT INTO table_for_update_field_dictionary VALUES (3, 'Third', now());")

    time.sleep(10)

    query_result = node.query("SELECT key, value FROM {dictionary_name} ORDER BY key ASC".format(dictionary_name=dictionary_name))

    assert query_result == '1\tFirst\n2\tSecondUpdated\n3\tThird\n'

    node.query("TRUNCATE TABLE table_for_update_field_dictionary")
    node.query("DROP DICTIONARY {dictionary_name}".format(dictionary_name=dictionary_name))
