from __future__ import print_function
import pytest
import os
import random
import string
import time

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__, base_configs_dir=os.path.join(SCRIPT_DIR, 'configs'))

dictionary_node = cluster.add_instance('dictionary_node', stay_alive=True)
main_node = cluster.add_instance('main_node', main_configs=['configs/dictionaries/cache_strings_default_settings.xml'])


def get_random_string(string_length=8):
    alphabet = string.ascii_letters + string.digits
    return ''.join((random.choice(alphabet) for _ in range(string_length)))

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        dictionary_node.query("CREATE DATABASE IF NOT EXISTS test;")
        dictionary_node.query("DROP TABLE IF EXISTS test.strings;")
        dictionary_node.query("""
                             CREATE TABLE test.strings 
                             (key UInt64, value String)
                             ENGINE = Memory;
                             """)

        values_to_insert = ", ".join(["({}, '{}')".format(1000000 + number, get_random_string()) for number in range(100)])
        dictionary_node.query("INSERT INTO test.strings VALUES {}".format(values_to_insert))

        yield cluster
    finally:
        cluster.shutdown()

# @pytest.mark.skip(reason="debugging")
def test_return_real_values(started_cluster):
    assert None != dictionary_node.get_process_pid("clickhouse"), "ClickHouse must be alive"

    first_batch = """
    SELECT count(*)
    FROM
    (
    SELECT
        arrayJoin(arrayMap(x -> (x + 1000000), range(100))) AS id,
        dictGetString('default_string', 'value', toUInt64(id)) AS value
    )
    WHERE value = '';
    """

    assert TSV("0") == TSV(main_node.query(first_batch))

    # Waiting for cache to become expired
    time.sleep(5)

    assert TSV("0") == TSV(main_node.query(first_batch))
