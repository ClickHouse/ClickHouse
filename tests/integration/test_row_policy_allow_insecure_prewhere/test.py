# pylint: disable=line-too-long

import re
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", config_dir="configs")

def strip(s):
    return re.sub(r'[\s]+', ' ', s.strip())

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        node.query('''
            CREATE DATABASE mydb ENGINE=Ordinary;

            CREATE TABLE mydb.prewhere_filter (a UInt8, b UInt8, c UInt8, s String) ENGINE MergeTree ORDER BY a PARTITION BY a SETTINGS index_granularity=1;
            INSERT INTO mydb.prewhere_filter SELECT number, number, number, randomPrintableASCII(1000) FROM numbers(10);

            CREATE TABLE mydb.prewhere_no_filter (a UInt8, b UInt8, c UInt8, s String) ENGINE MergeTree ORDER BY a PARTITION BY a SETTINGS index_granularity=1;
            INSERT INTO mydb.prewhere_no_filter SELECT number, number, number, randomPrintableASCII(1000) FROM numbers(10);
        ''')

        yield cluster

    finally:
        cluster.shutdown()


def test_PREWHERE():
    settings = {
        'max_threads': 1,
        'optimize_move_to_prewhere': 1,
        # enough to trigger an error if PREWHERE does not works
        'max_bytes_to_read': 1000 + 30,
    }

    # Check that filter does works
    assert int(node.query("SELECT count() FROM mydb.prewhere_no_filter")) == 10
    assert int(node.query("SELECT count() FROM mydb.prewhere_filter")) == 1

    # PREWHERE
    node.query("SELECT * FROM mydb.prewhere_no_filter PREWHERE c = 3 AND b = 3 FORMAT Null", settings=settings)
    node.query("SELECT * FROM mydb.prewhere_filter    PREWHERE c = 3 FORMAT Null", settings=settings)

    # WHERE w/ optimize_move_to_prewhere
    node.query("SELECT * FROM mydb.prewhere_no_filter WHERE c = 3 AND b = 3 FORMAT Null", settings=settings)
    node.query("SELECT * FROM mydb.prewhere_filter    WHERE c = 3 FORMAT Null", settings=settings)

    # PREWHERE and WHERE w/ optimize_move_to_prewhere
    node.query("SELECT s FROM mydb.prewhere_filter PREWHERE 1 WHERE c = 3 FORMAT Null", settings=settings)
    # EXPLAIN PREWHERE and WHERE w/ optimize_move_to_prewhere
    assert strip(node.query("EXPLAIN SYNTAX SELECT s FROM mydb.prewhere_no_filter WHERE b = 3 AND 1 = 1 AND c = 3", settings=settings)) == \
        strip("""
        SELECT s
        FROM mydb.prewhere_no_filter
        PREWHERE (b = 3) AND (c = 3)
        """)
    assert strip(node.query("EXPLAIN SYNTAX SELECT s FROM mydb.prewhere_filter WHERE c = 3", settings=settings)) == \
        strip("""
        SELECT s
        FROM mydb.prewhere_filter
        PREWHERE (b = 3) AND (c = 3)
        """)

    # WHERE w/o optimize_move_to_prewhere (just make sure it works)
    node.query("SELECT * FROM mydb.prewhere_no_filter WHERE a = 3 AND b = 3 FORMAT Null", settings={'optimize_move_to_prewhere': 0})
    node.query("SELECT * FROM mydb.prewhere_filter    WHERE a = 3 FORMAT Null", settings={'optimize_move_to_prewhere': 0})
