import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
import os
import re
import time

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                config_dir="configs")


def copy_policy_xml(local_file_name, reload_immediately = True):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    instance.copy_file_to_container(os.path.join(script_dir, local_file_name), '/etc/clickhouse-server/users.d/row_policy.xml')
    if reload_immediately:
       instance.query("SYSTEM RELOAD CONFIG")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        instance.query('''
            CREATE DATABASE mydb;

            CREATE TABLE mydb.filtered_table1 (a UInt8, b UInt8) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.filtered_table1 values (0, 0), (0, 1), (1, 0), (1, 1);

            CREATE TABLE mydb.filtered_table2 (a UInt8, b UInt8, c UInt8, d UInt8) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.filtered_table2 values (0, 0, 0, 0), (1, 2, 3, 4), (4, 3, 2, 1), (0, 0, 6, 0);

            CREATE TABLE mydb.filtered_table3 (a UInt8, b UInt8, c UInt16 ALIAS a + b) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.filtered_table3 values (0, 0), (0, 1), (1, 0), (1, 1);
        ''')
        
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def reset_policies():
    try:
        yield
    finally:
        copy_policy_xml('normal_filters.xml')
        instance.query("DROP POLICY IF EXISTS pA, pB ON mydb.filtered_table1")


def test_smoke():
    assert instance.query("SELECT * FROM mydb.filtered_table1") == "1\t0\n1\t1\n"
    assert instance.query("SELECT * FROM mydb.filtered_table2") == "0\t0\t0\t0\n0\t0\t6\t0\n"
    assert instance.query("SELECT * FROM mydb.filtered_table3") == "0\t1\n1\t0\n"

    assert instance.query("SELECT a FROM mydb.filtered_table1") == "1\n1\n"
    assert instance.query("SELECT b FROM mydb.filtered_table1") == "0\n1\n"
    assert instance.query("SELECT a FROM mydb.filtered_table1 WHERE a = 1") == "1\n1\n"
    assert instance.query("SELECT a = 1 FROM mydb.filtered_table1") == "1\n1\n"

    assert instance.query("SELECT a FROM mydb.filtered_table3") == "0\n1\n"
    assert instance.query("SELECT b FROM mydb.filtered_table3") == "1\n0\n"
    assert instance.query("SELECT c FROM mydb.filtered_table3") == "1\n1\n"
    assert instance.query("SELECT a + b FROM mydb.filtered_table3") == "1\n1\n"
    assert instance.query("SELECT a FROM mydb.filtered_table3 WHERE c = 1") == "0\n1\n"
    assert instance.query("SELECT c = 1 FROM mydb.filtered_table3") == "1\n1\n"
    assert instance.query("SELECT a + b = 1 FROM mydb.filtered_table3") == "1\n1\n"


def test_join():
    assert instance.query("SELECT * FROM mydb.filtered_table1 as t1 ANY LEFT JOIN mydb.filtered_table1 as t2 ON t1.a = t2.b") == "1\t0\t1\t1\n1\t1\t1\t1\n"
    assert instance.query("SELECT * FROM mydb.filtered_table1 as t2 ANY RIGHT JOIN mydb.filtered_table1 as t1 ON t2.b = t1.a") == "1\t1\t1\t0\n"


def test_cannot_trick_row_policy_with_keyword_with():
    assert instance.query("WITH 0 AS a SELECT * FROM mydb.filtered_table1") == "1\t0\n1\t1\n"
    assert instance.query("WITH 0 AS a SELECT a, b FROM mydb.filtered_table1") == "1\t0\n1\t1\n"
    assert instance.query("WITH 0 AS a SELECT a FROM mydb.filtered_table1") == "1\n1\n"
    assert instance.query("WITH 0 AS a SELECT b FROM mydb.filtered_table1") == "0\n1\n"


def test_prewhere_not_supported():
    expected_error = "PREWHERE is not supported if the table is filtered by row-level security"
    assert expected_error in instance.query_and_get_error("SELECT * FROM mydb.filtered_table1 PREWHERE 1")
    assert expected_error in instance.query_and_get_error("SELECT * FROM mydb.filtered_table2 PREWHERE 1")
    assert expected_error in instance.query_and_get_error("SELECT * FROM mydb.filtered_table3 PREWHERE 1")


def test_change_of_users_xml_changes_row_policies():
    copy_policy_xml('normal_filters.xml')
    assert instance.query("SELECT * FROM mydb.filtered_table1") == "1\t0\n1\t1\n"
    assert instance.query("SELECT * FROM mydb.filtered_table2") == "0\t0\t0\t0\n0\t0\t6\t0\n"
    assert instance.query("SELECT * FROM mydb.filtered_table3") == "0\t1\n1\t0\n"

    copy_policy_xml('all_rows.xml')
    assert instance.query("SELECT * FROM mydb.filtered_table1") == "0\t0\n0\t1\n1\t0\n1\t1\n"
    assert instance.query("SELECT * FROM mydb.filtered_table2") == "0\t0\t0\t0\n0\t0\t6\t0\n1\t2\t3\t4\n4\t3\t2\t1\n"
    assert instance.query("SELECT * FROM mydb.filtered_table3") == "0\t0\n0\t1\n1\t0\n1\t1\n"

    copy_policy_xml('no_rows.xml')
    assert instance.query("SELECT * FROM mydb.filtered_table1") == ""
    assert instance.query("SELECT * FROM mydb.filtered_table2") == ""
    assert instance.query("SELECT * FROM mydb.filtered_table3") == ""

    copy_policy_xml('normal_filters.xml')
    assert instance.query("SELECT * FROM mydb.filtered_table1") == "1\t0\n1\t1\n"
    assert instance.query("SELECT * FROM mydb.filtered_table2") == "0\t0\t0\t0\n0\t0\t6\t0\n"
    assert instance.query("SELECT * FROM mydb.filtered_table3") == "0\t1\n1\t0\n"

    copy_policy_xml('no_filters.xml')
    assert instance.query("SELECT * FROM mydb.filtered_table1") == "0\t0\n0\t1\n1\t0\n1\t1\n"
    assert instance.query("SELECT * FROM mydb.filtered_table2") == "0\t0\t0\t0\n0\t0\t6\t0\n1\t2\t3\t4\n4\t3\t2\t1\n"
    assert instance.query("SELECT * FROM mydb.filtered_table3") == "0\t0\n0\t1\n1\t0\n1\t1\n"

    copy_policy_xml('normal_filters.xml')
    assert instance.query("SELECT * FROM mydb.filtered_table1") == "1\t0\n1\t1\n"
    assert instance.query("SELECT * FROM mydb.filtered_table2") == "0\t0\t0\t0\n0\t0\t6\t0\n"
    assert instance.query("SELECT * FROM mydb.filtered_table3") == "0\t1\n1\t0\n"


def test_reload_users_xml_by_timer():
    copy_policy_xml('normal_filters.xml')
    assert instance.query("SELECT * FROM mydb.filtered_table1") == "1\t0\n1\t1\n"
    assert instance.query("SELECT * FROM mydb.filtered_table2") == "0\t0\t0\t0\n0\t0\t6\t0\n"
    assert instance.query("SELECT * FROM mydb.filtered_table3") == "0\t1\n1\t0\n"

    time.sleep(1) # The modification time of the 'row_policy.xml' file should be different.
    copy_policy_xml('all_rows.xml', False)
    assert_eq_with_retry(instance, "SELECT * FROM mydb.filtered_table1", "0\t0\n0\t1\n1\t0\n1\t1")
    assert_eq_with_retry(instance, "SELECT * FROM mydb.filtered_table2", "0\t0\t0\t0\n0\t0\t6\t0\n1\t2\t3\t4\n4\t3\t2\t1")
    assert_eq_with_retry(instance, "SELECT * FROM mydb.filtered_table3", "0\t0\n0\t1\n1\t0\n1\t1")

    time.sleep(1) # The modification time of the 'row_policy.xml' file should be different.
    copy_policy_xml('normal_filters.xml', False)
    assert_eq_with_retry(instance, "SELECT * FROM mydb.filtered_table1", "1\t0\n1\t1")
    assert_eq_with_retry(instance, "SELECT * FROM mydb.filtered_table2", "0\t0\t0\t0\n0\t0\t6\t0")
    assert_eq_with_retry(instance, "SELECT * FROM mydb.filtered_table3", "0\t1\n1\t0")
