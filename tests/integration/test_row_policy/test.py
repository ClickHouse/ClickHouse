import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
import os
import re
import time

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance1', config_dir="configs", with_zookeeper=True)
instance2 = cluster.add_instance('instance2', config_dir="configs", with_zookeeper=True)


def copy_policy_xml(local_file_name, reload_immediately = True):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    instance.copy_file_to_container(os.path.join(script_dir, local_file_name), '/etc/clickhouse-server/users.d/row_policy.xml')
    instance2.copy_file_to_container(os.path.join(script_dir, local_file_name), '/etc/clickhouse-server/users.d/row_policy.xml')
    if reload_immediately:
       instance.query("SYSTEM RELOAD CONFIG")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        instance.query('''
            CREATE DATABASE mydb ENGINE=Ordinary;

            CREATE TABLE mydb.filtered_table1 (a UInt8, b UInt8) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.filtered_table1 values (0, 0), (0, 1), (1, 0), (1, 1);

            CREATE TABLE mydb.table (a UInt8, b UInt8) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.table values (0, 0), (0, 1), (1, 0), (1, 1);

            CREATE TABLE mydb.filtered_table2 (a UInt8, b UInt8, c UInt8, d UInt8) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.filtered_table2 values (0, 0, 0, 0), (1, 2, 3, 4), (4, 3, 2, 1), (0, 0, 6, 0);

            CREATE TABLE mydb.filtered_table3 (a UInt8, b UInt8, c UInt16 ALIAS a + b) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.filtered_table3 values (0, 0), (0, 1), (1, 0), (1, 1);

            CREATE TABLE mydb.`.filtered_table4` (a UInt8, b UInt8, c UInt16 ALIAS a + b) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.`.filtered_table4` values (0, 0), (0, 1), (1, 0), (1, 1);
        ''')
        instance2.query('''
            CREATE DATABASE mydb ENGINE=Ordinary;

            CREATE TABLE mydb.filtered_table1 (a UInt8, b UInt8) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.filtered_table1 values (0, 0), (0, 1), (1, 0), (1, 1);

            CREATE TABLE mydb.table (a UInt8, b UInt8) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.table values (0, 0), (0, 1), (1, 0), (1, 1);

            CREATE TABLE mydb.filtered_table2 (a UInt8, b UInt8, c UInt8, d UInt8) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.filtered_table2 values (0, 0, 0, 0), (1, 2, 3, 4), (4, 3, 2, 1), (0, 0, 6, 0);

            CREATE TABLE mydb.filtered_table3 (a UInt8, b UInt8, c UInt16 ALIAS a + b) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.filtered_table3 values (0, 0), (0, 1), (1, 0), (1, 1);

            CREATE TABLE mydb.`.filtered_table4` (a UInt8, b UInt8, c UInt16 ALIAS a + b) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.`.filtered_table4` values (0, 0), (0, 1), (1, 0), (1, 1);
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
    assert instance.query("SELECT a FROM mydb.filtered_table1 WHERE a IN (1)") == "1\n1\n"
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
    assert instance.query("WITH 0 AS a SELECT a, b FROM mydb.filtered_table1") == "0\t0\n0\t1\n"
    assert instance.query("WITH 0 AS a SELECT a FROM mydb.filtered_table1") == "0\n0\n"
    assert instance.query("WITH 0 AS a SELECT b FROM mydb.filtered_table1") == "0\n1\n"


def test_prewhere_not_supported():
    expected_error = "PREWHERE is not supported if the table is filtered by row-level security"
    assert expected_error in instance.query_and_get_error("SELECT * FROM mydb.filtered_table1 PREWHERE 1")
    assert expected_error in instance.query_and_get_error("SELECT * FROM mydb.filtered_table2 PREWHERE 1")
    assert expected_error in instance.query_and_get_error("SELECT * FROM mydb.filtered_table3 PREWHERE 1")

    # However PREWHERE should still work for user without filtering.
    assert instance.query("SELECT * FROM mydb.filtered_table1 PREWHERE 1", user="another") == "0\t0\n0\t1\n1\t0\n1\t1\n"


def test_single_table_name():
    copy_policy_xml('tag_with_table_name.xml')
    assert instance.query("SELECT * FROM mydb.table") == "1\t0\n1\t1\n"
    assert instance.query("SELECT * FROM mydb.filtered_table2") == "0\t0\t0\t0\n0\t0\t6\t0\n"
    assert instance.query("SELECT * FROM mydb.filtered_table3") == "0\t1\n1\t0\n"

    assert instance.query("SELECT a FROM mydb.table") == "1\n1\n"
    assert instance.query("SELECT b FROM mydb.table") == "0\n1\n"
    assert instance.query("SELECT a FROM mydb.table WHERE a = 1") == "1\n1\n"
    assert instance.query("SELECT a = 1 FROM mydb.table") == "1\n1\n"

    assert instance.query("SELECT a FROM mydb.filtered_table3") == "0\n1\n"
    assert instance.query("SELECT b FROM mydb.filtered_table3") == "1\n0\n"
    assert instance.query("SELECT c FROM mydb.filtered_table3") == "1\n1\n"
    assert instance.query("SELECT a + b FROM mydb.filtered_table3") == "1\n1\n"
    assert instance.query("SELECT a FROM mydb.filtered_table3 WHERE c = 1") == "0\n1\n"
    assert instance.query("SELECT c = 1 FROM mydb.filtered_table3") == "1\n1\n"
    assert instance.query("SELECT a + b = 1 FROM mydb.filtered_table3") == "1\n1\n"


def test_custom_table_name():
    copy_policy_xml('multiple_tags_with_table_names.xml')
    assert instance.query("SELECT * FROM mydb.table") == "1\t0\n1\t1\n"
    assert instance.query("SELECT * FROM mydb.filtered_table2") == "0\t0\t0\t0\n0\t0\t6\t0\n"
    assert instance.query("SELECT * FROM mydb.`.filtered_table4`") == "0\t1\n1\t0\n"

    assert instance.query("SELECT a FROM mydb.table") == "1\n1\n"
    assert instance.query("SELECT b FROM mydb.table") == "0\n1\n"
    assert instance.query("SELECT a FROM mydb.table WHERE a = 1") == "1\n1\n"
    assert instance.query("SELECT a = 1 FROM mydb.table") == "1\n1\n"

    assert instance.query("SELECT a FROM mydb.`.filtered_table4`") == "0\n1\n"
    assert instance.query("SELECT b FROM mydb.`.filtered_table4`") == "1\n0\n"
    assert instance.query("SELECT c FROM mydb.`.filtered_table4`") == "1\n1\n"
    assert instance.query("SELECT a + b FROM mydb.`.filtered_table4`") == "1\n1\n"
    assert instance.query("SELECT a FROM mydb.`.filtered_table4` WHERE c = 1") == "0\n1\n"
    assert instance.query("SELECT c = 1 FROM mydb.`.filtered_table4`") == "1\n1\n"
    assert instance.query("SELECT a + b = 1 FROM mydb.`.filtered_table4`") == "1\n1\n"


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


def test_introspection():
    assert instance.query("SELECT currentRowPolicies('mydb', 'filtered_table1')") == "['default']\n"
    assert instance.query("SELECT currentRowPolicies('mydb', 'filtered_table2')") == "['default']\n"
    assert instance.query("SELECT currentRowPolicies('mydb', 'filtered_table3')") == "['default']\n"
    assert instance.query("SELECT arraySort(currentRowPolicies())") == "[('mydb','filtered_table1','default'),('mydb','filtered_table2','default'),('mydb','filtered_table3','default'),('mydb','local','default')]\n"

    policy1 = "mydb\tfiltered_table1\tdefault\tdefault ON mydb.filtered_table1\t9e8a8f62-4965-2b5e-8599-57c7b99b3549\tusers.xml\t0\ta = 1\t\t\t\t\n"
    policy2 = "mydb\tfiltered_table2\tdefault\tdefault ON mydb.filtered_table2\tcffae79d-b9bf-a2ef-b798-019c18470b25\tusers.xml\t0\ta + b < 1 or c - d > 5\t\t\t\t\n"
    policy3 = "mydb\tfiltered_table3\tdefault\tdefault ON mydb.filtered_table3\t12fc5cef-e3da-3940-ec79-d8be3911f42b\tusers.xml\t0\tc = 1\t\t\t\t\n"
    policy4 = "mydb\tlocal\tdefault\tdefault ON mydb.local\tcdacaeb5-1d97-f99d-2bb0-4574f290629c\tusers.xml\t0\t1\t\t\t\t\n"
    assert instance.query("SELECT * from system.row_policies WHERE has(currentRowPolicyIDs('mydb', 'filtered_table1'), id) ORDER BY table, name") == policy1
    assert instance.query("SELECT * from system.row_policies WHERE has(currentRowPolicyIDs('mydb', 'filtered_table2'), id) ORDER BY table, name") == policy2
    assert instance.query("SELECT * from system.row_policies WHERE has(currentRowPolicyIDs('mydb', 'filtered_table3'), id) ORDER BY table, name") == policy3
    assert instance.query("SELECT * from system.row_policies WHERE has(currentRowPolicyIDs('mydb', 'local'), id) ORDER BY table, name") == policy4
    assert instance.query("SELECT * from system.row_policies WHERE has(currentRowPolicyIDs(), id) ORDER BY table, name") == policy1 + policy2 + policy3 + policy4


def test_dcl_introspection():
    assert instance.query("SHOW POLICIES ON mydb.filtered_table1") == "another\ndefault\n"
    assert instance.query("SHOW POLICIES CURRENT ON mydb.filtered_table2") == "default\n"
    assert instance.query("SHOW POLICIES") == "another ON mydb.filtered_table1\nanother ON mydb.filtered_table2\nanother ON mydb.filtered_table3\nanother ON mydb.local\ndefault ON mydb.filtered_table1\ndefault ON mydb.filtered_table2\ndefault ON mydb.filtered_table3\ndefault ON mydb.local\n"
    assert instance.query("SHOW POLICIES CURRENT") == "default ON mydb.filtered_table1\ndefault ON mydb.filtered_table2\ndefault ON mydb.filtered_table3\ndefault ON mydb.local\n"

    assert instance.query("SHOW CREATE POLICY default ON mydb.filtered_table1") == "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING a = 1 TO default\n"
    assert instance.query("SHOW CREATE POLICY default ON mydb.filtered_table2") == "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING ((a + b) < 1) OR ((c - d) > 5) TO default\n"
    assert instance.query("SHOW CREATE POLICY default ON mydb.filtered_table3") == "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING c = 1 TO default\n"
    assert instance.query("SHOW CREATE POLICY default ON mydb.local") == "CREATE ROW POLICY default ON mydb.local FOR SELECT USING 1 TO default\n"

    copy_policy_xml('all_rows.xml')
    assert instance.query("SHOW POLICIES CURRENT") == "default ON mydb.filtered_table1\ndefault ON mydb.filtered_table2\ndefault ON mydb.filtered_table3\n"
    assert instance.query("SHOW CREATE POLICY default ON mydb.filtered_table1") == "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING 1 TO default\n"
    assert instance.query("SHOW CREATE POLICY default ON mydb.filtered_table2") == "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING 1 TO default\n"
    assert instance.query("SHOW CREATE POLICY default ON mydb.filtered_table3") == "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING 1 TO default\n"

    copy_policy_xml('no_rows.xml')
    assert instance.query("SHOW POLICIES CURRENT") == "default ON mydb.filtered_table1\ndefault ON mydb.filtered_table2\ndefault ON mydb.filtered_table3\n"
    assert instance.query("SHOW CREATE POLICY default ON mydb.filtered_table1") == "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING NULL TO default\n"
    assert instance.query("SHOW CREATE POLICY default ON mydb.filtered_table2") == "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING NULL TO default\n"
    assert instance.query("SHOW CREATE POLICY default ON mydb.filtered_table3") == "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING NULL TO default\n"

    copy_policy_xml('no_filters.xml')
    assert instance.query("SHOW POLICIES") == ""


def test_dcl_management():
    copy_policy_xml('no_filters.xml')
    assert instance.query("SHOW POLICIES") == ""

    instance.query("CREATE POLICY pA ON mydb.filtered_table1 FOR SELECT USING a<b")
    assert instance.query("SELECT * FROM mydb.filtered_table1") == ""
    assert instance.query("SHOW POLICIES CURRENT ON mydb.filtered_table1") == ""
    assert instance.query("SHOW POLICIES ON mydb.filtered_table1") == "pA\n"

    instance.query("ALTER POLICY pA ON mydb.filtered_table1 TO default")
    assert instance.query("SELECT * FROM mydb.filtered_table1") == "0\t1\n"
    assert instance.query("SHOW POLICIES CURRENT ON mydb.filtered_table1") == "pA\n"

    instance.query("ALTER POLICY pA ON mydb.filtered_table1 FOR SELECT USING a>b")
    assert instance.query("SELECT * FROM mydb.filtered_table1") == "1\t0\n"

    instance.query("ALTER POLICY pA ON mydb.filtered_table1 RENAME TO pB")
    assert instance.query("SELECT * FROM mydb.filtered_table1") == "1\t0\n"
    assert instance.query("SHOW POLICIES CURRENT ON mydb.filtered_table1") == "pB\n"
    assert instance.query("SHOW CREATE POLICY pB ON mydb.filtered_table1") == "CREATE ROW POLICY pB ON mydb.filtered_table1 FOR SELECT USING a > b TO default\n"

    instance.query("DROP POLICY pB ON mydb.filtered_table1")
    assert instance.query("SELECT * FROM mydb.filtered_table1") == "0\t0\n0\t1\n1\t0\n1\t1\n"
    assert instance.query("SHOW POLICIES") == ""


def test_users_xml_is_readonly():
    assert re.search("storage is readonly", instance.query_and_get_error("DROP POLICY default ON mydb.filtered_table1"))


def test_miscellaneous_engines():
    copy_policy_xml('normal_filters.xml')

    # ReplicatedMergeTree
    instance.query("DROP TABLE mydb.filtered_table1")
    instance.query("CREATE TABLE mydb.filtered_table1 (a UInt8, b UInt8) ENGINE ReplicatedMergeTree('/clickhouse/tables/00-00/filtered_table1', 'replica1') ORDER BY a")
    instance.query("INSERT INTO mydb.filtered_table1 values (0, 0), (0, 1), (1, 0), (1, 1)")
    assert instance.query("SELECT * FROM mydb.filtered_table1") == "1\t0\n1\t1\n"

    # CollapsingMergeTree
    instance.query("DROP TABLE mydb.filtered_table1")
    instance.query("CREATE TABLE mydb.filtered_table1 (a UInt8, b Int8) ENGINE CollapsingMergeTree(b) ORDER BY a")
    instance.query("INSERT INTO mydb.filtered_table1 values (0, 1), (0, 1), (1, 1), (1, 1)")
    assert instance.query("SELECT * FROM mydb.filtered_table1") == "1\t1\n1\t1\n"

    # ReplicatedCollapsingMergeTree
    instance.query("DROP TABLE mydb.filtered_table1")
    instance.query("CREATE TABLE mydb.filtered_table1 (a UInt8, b Int8) ENGINE ReplicatedCollapsingMergeTree('/clickhouse/tables/00-00/filtered_table1', 'replica1', b) ORDER BY a")
    instance.query("INSERT INTO mydb.filtered_table1 values (0, 1), (0, 1), (1, 1), (1, 1)")
    assert instance.query("SELECT * FROM mydb.filtered_table1") == "1\t1\n1\t1\n"

    # DistributedMergeTree
    instance.query("DROP TABLE IF EXISTS mydb.not_filtered_table")
    instance.query("CREATE TABLE mydb.not_filtered_table (a UInt8, b UInt8) ENGINE Distributed('test_local_cluster', mydb, local)")
    instance.query("CREATE TABLE mydb.local (a UInt8, b UInt8) ENGINE MergeTree ORDER BY a")
    instance2.query("CREATE TABLE mydb.local (a UInt8, b UInt8) ENGINE MergeTree ORDER BY a")
    instance.query("INSERT INTO mydb.local values (2, 0), (2, 1), (1, 0), (1, 1)")
    instance2.query("INSERT INTO mydb.local values (3, 0), (3, 1), (1, 0), (1, 1)")
    assert instance.query("SELECT * FROM mydb.not_filtered_table", user="another") == "1\t0\n1\t1\n1\t0\n1\t1\n"
    assert instance.query("SELECT sum(a), b FROM mydb.not_filtered_table GROUP BY b ORDER BY b", user="another") == "2\t0\n2\t1\n"
