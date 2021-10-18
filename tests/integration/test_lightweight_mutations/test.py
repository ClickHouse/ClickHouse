import os
import time
from multiprocessing.dummy import Pool

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance('node')


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def check_hardlinks(table, part_path, column_file, count):
    column_path = os.path.join("/var/lib/clickhouse/data/default", table, part_path, column_file)
    script = """
        export INODE=`ls -i {column_path} | awk '{{print $1}}'`
        export COUNT=`find /var/lib/clickhouse -inum $INODE | wc -l`
        test $COUNT = {count}
    """.format(column_path=column_path, count=count)

    node.exec_in_container(["bash", "-c", script])


def check_exists(table, part_path, column_file):
    column_path = os.path.join("/var/lib/clickhouse/data/default", table, part_path, column_file)

    node.exec_in_container(["bash", "-c", "test -f {}".format(column_path)])


def create_table():
    node.query("CREATE TABLE table (key UInt64) ENGINE MergeTree() ORDER BY tuple() PARTITION BY key % 10")
    node.query("INSERT INTO table SELECT number FROM numbers(100)")

    assert_eq_with_retry(node, "SELECT sum(key) FROM table", str(sum(range(100))))

    node.query("SYSTEM STOP MERGES")


def wait_for_lwmutation(result):
    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.mutations WHERE table='table' AND is_lightweight AND !is_done",
        result, retry_count=100)



def test_lwmutation_with_ordinary_mutations(started_cluster):
    """
    1. You can mix ordinary and lightweight mutations. Each mutation gets a new block number regardless of its type.
    2. Server evaluates a mix of ordinary and lightweight mutations correctly.
    """
    pass


def test_lwmutation_many_mutations(started_cluster):
    """
    If many lightweight mutations are run, each of them creates masks for parts with unique block numbers.
    """
    pass

def test_lwmutation_parallel(started_cluster):
    """
    Lightweight mutations can be launched in parallel.
    """

    create_table()

    p = Pool(2)
    p.apply_async(lambda: node.query("DELETE FROM table WHERE key % 2 == 0"))

    wait_for_lwmutation("2")


def test_lwmutation_basic(started_cluster):
    """
    1. A lightweight mutation creates masks for each active part.
    2. Masks persist on disk and can be loaded on server startup.
    """
    create_table()

    node.query("DELETE FROM table WHERE key % 2 == 0")

    wait_for_lwmutation("1")

    node.query("SYSTEM START MERGES")

    assert_eq_with_retry(node, "SELECT count() FROM table", str(sum(1 for i in range(100) if i % 2 != 0)))

    check_hardlinks("table_for_delete_and_drop", "all_1_1_0_3", "key.bin", 1)
    check_hardlinks("table_for_delete_and_drop", "all_1_1_0_3", "value1.bin", 1)

    with pytest.raises(Exception):
        check_exists("table_for_delete_and_drop", "all_1_1_0_3", "value2.bin")
    with pytest.raises(Exception):
        check_exists("table_for_delete_and_drop", "all_1_1_0_3", "value2.mrk")
