import pytest

import os
import time
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from multiprocessing.dummy import Pool


cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1')

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

    node1.exec_in_container(["bash", "-c", script])


def check_exists(table, part_path, column_file):
    column_path = os.path.join("/var/lib/clickhouse/data/default", table, part_path, column_file)

    node1.exec_in_container(["bash", "-c", "test -f {}".format(column_path)])


def test_update_mutation(started_cluster):
    node1.query("CREATE TABLE table_for_update(key UInt64, value1 UInt64, value2 String) ENGINE MergeTree() ORDER BY tuple()")

    node1.query("INSERT INTO table_for_update SELECT number, number, toString(number) from numbers(100)")

    assert int(node1.query("SELECT sum(value1) FROM table_for_update").strip()) == sum(range(100))

    node1.query("ALTER TABLE table_for_update UPDATE value1 = value1 * value1 WHERE 1", settings={"mutations_sync" : "2"})
    assert int(node1.query("SELECT sum(value1) FROM table_for_update").strip()) == sum(i * i for i in range(100))

    check_hardlinks("table_for_update", "all_1_1_0_2", "key.bin", 2)
    check_hardlinks("table_for_update", "all_1_1_0_2", "value2.bin", 2)
    check_hardlinks("table_for_update", "all_1_1_0_2", "value1.bin", 1)

    node1.query("ALTER TABLE table_for_update UPDATE key=key, value1=value1, value2=value2 WHERE 1", settings={"mutations_sync": "2"})

    assert int(node1.query("SELECT sum(value1) FROM table_for_update").strip()) == sum(i * i for i in range(100))

    check_hardlinks("table_for_update", "all_1_1_0_3", "key.bin", 1)
    check_hardlinks("table_for_update", "all_1_1_0_3", "value1.bin", 1)
    check_hardlinks("table_for_update", "all_1_1_0_3", "value2.bin", 1)


def test_modify_mutation(started_cluster):
    node1.query("CREATE TABLE table_for_modify(key UInt64, value1 UInt64, value2 String) ENGINE MergeTree() ORDER BY tuple()")

    node1.query("INSERT INTO table_for_modify SELECT number, number, toString(number) from numbers(100)")

    assert int(node1.query("SELECT sum(value1) FROM table_for_modify").strip()) == sum(range(100))

    node1.query("ALTER TABLE table_for_modify MODIFY COLUMN value2 UInt64", settings={"mutations_sync" : "2"})

    assert int(node1.query("SELECT sum(value2) FROM table_for_modify").strip()) == sum(range(100))

    check_hardlinks("table_for_modify", "all_1_1_0_2", "key.bin", 2)
    check_hardlinks("table_for_modify", "all_1_1_0_2", "value1.bin", 2)
    check_hardlinks("table_for_modify", "all_1_1_0_2", "value2.bin", 1)


def test_drop_mutation(started_cluster):
    node1.query("CREATE TABLE table_for_drop(key UInt64, value1 UInt64, value2 String) ENGINE MergeTree() ORDER BY tuple()")

    node1.query("INSERT INTO table_for_drop SELECT number, number, toString(number) from numbers(100)")

    assert int(node1.query("SELECT sum(value1) FROM table_for_drop").strip()) == sum(range(100))

    node1.query("ALTER TABLE table_for_drop DROP COLUMN value2", settings={"mutations_sync": "2"})

    check_hardlinks("table_for_drop", "all_1_1_0_2", "key.bin", 2)
    check_hardlinks("table_for_drop", "all_1_1_0_2", "value1.bin", 2)

    with pytest.raises(Exception):
        check_exists("table_for_drop", "all_1_1_0_2", "value2.bin")
    with pytest.raises(Exception):
        check_exists("table_for_drop", "all_1_1_0_2", "value2.mrk")


def test_delete_and_drop_mutation(started_cluster):
    node1.query("CREATE TABLE table_for_delete_and_drop(key UInt64, value1 UInt64, value2 String) ENGINE MergeTree() ORDER BY tuple()")

    node1.query("INSERT INTO table_for_delete_and_drop SELECT number, number, toString(number) from numbers(100)")

    assert int(node1.query("SELECT sum(value1) FROM table_for_delete_and_drop").strip()) == sum(range(100))

    node1.query("SYSTEM STOP MERGES")

    def mutate():
        node1.query("ALTER TABLE table_for_delete_and_drop DELETE WHERE key % 2 == 0, DROP COLUMN value2")

    p = Pool(2)
    p.apply_async(mutate)

    for _ in range(1, 100):
        result = node1.query("SELECT COUNT() FROM system.mutations WHERE table = 'table_for_delete_and_drop' and is_done=0")
        try:
            if int(result.strip()) == 2:
                break
        except:
            print "Result", result
            pass

        time.sleep(0.5)

    node1.query("SYSTEM START MERGES")

    assert_eq_with_retry(node1, "SELECT COUNT() FROM table_for_delete_and_drop", str(sum(1 for i in range(100) if i % 2 != 0)))

    check_hardlinks("table_for_delete_and_drop", "all_1_1_0_3", "key.bin", 1)
    check_hardlinks("table_for_delete_and_drop", "all_1_1_0_3", "value1.bin", 1)

    with pytest.raises(Exception):
        check_exists("table_for_delete_and_drop", "all_1_1_0_3", "value2.bin")
    with pytest.raises(Exception):
        check_exists("table_for_delete_and_drop", "all_1_1_0_3", "value2.mrk")
