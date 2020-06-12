import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, TSV
import os
import re
import time

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', config_dir="configs", with_zookeeper=True)
node2 = cluster.add_instance('node2', config_dir="configs", with_zookeeper=True)
nodes = [node, node2]


def copy_policy_xml(local_file_name, reload_immediately = True):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    for current_node in nodes:
        current_node.copy_file_to_container(os.path.join(script_dir, local_file_name), '/etc/clickhouse-server/users.d/row_policy.xml')
        if reload_immediately:
            current_node.query("SYSTEM RELOAD CONFIG")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        for current_node in nodes:
            current_node.query('''
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
                
                CREATE TABLE mydb.local (a UInt8, b UInt8) ENGINE MergeTree ORDER BY a;
            ''')

        node.query("INSERT INTO mydb.local values (2, 0), (2, 1), (1, 0), (1, 1)")
        node2.query("INSERT INTO mydb.local values (3, 0), (3, 1), (1, 0), (1, 1)")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def reset_policies():
    try:
        yield
    finally:
        copy_policy_xml('normal_filters.xml')
        for current_node in nodes:
            current_node.query("DROP POLICY IF EXISTS pA, pB ON mydb.filtered_table1")


def test_smoke():
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1,0], [1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV([[0, 0, 0, 0], [0, 0, 6, 0]])
    assert node.query("SELECT * FROM mydb.filtered_table3") == TSV([[0, 1], [1, 0]])

    assert node.query("SELECT a FROM mydb.filtered_table1") == TSV([[1], [1]])
    assert node.query("SELECT b FROM mydb.filtered_table1") == TSV([[0], [1]])
    assert node.query("SELECT a FROM mydb.filtered_table1 WHERE a = 1") == TSV([[1], [1]])
    assert node.query("SELECT a FROM mydb.filtered_table1 WHERE a IN (1)") == TSV([[1], [1]])
    assert node.query("SELECT a = 1 FROM mydb.filtered_table1") == TSV([[1], [1]])

    assert node.query("SELECT a FROM mydb.filtered_table3") == TSV([[0], [1]])
    assert node.query("SELECT b FROM mydb.filtered_table3") == TSV([[1], [0]])
    assert node.query("SELECT c FROM mydb.filtered_table3") == TSV([[1], [1]])
    assert node.query("SELECT a + b FROM mydb.filtered_table3") == TSV([[1], [1]])
    assert node.query("SELECT a FROM mydb.filtered_table3 WHERE c = 1") == TSV([[0], [1]])
    assert node.query("SELECT c = 1 FROM mydb.filtered_table3") == TSV([[1], [1]])
    assert node.query("SELECT a + b = 1 FROM mydb.filtered_table3") == TSV([[1], [1]])


def test_join():
    assert node.query("SELECT * FROM mydb.filtered_table1 as t1 ANY LEFT JOIN mydb.filtered_table1 as t2 ON t1.a = t2.b") == TSV([[1, 0, 1, 1], [1, 1, 1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table1 as t2 ANY RIGHT JOIN mydb.filtered_table1 as t1 ON t2.b = t1.a") == TSV([[1, 1, 1, 0]])


def test_cannot_trick_row_policy_with_keyword_with():
    assert node.query("WITH 0 AS a SELECT * FROM mydb.filtered_table1") == TSV([[1, 0], [1, 1]])
    assert node.query("WITH 0 AS a SELECT a, b FROM mydb.filtered_table1") == TSV([[0, 0], [0, 1]])
    assert node.query("WITH 0 AS a SELECT a FROM mydb.filtered_table1") == TSV([[0], [0]])
    assert node.query("WITH 0 AS a SELECT b FROM mydb.filtered_table1") == TSV([[0], [1]])


def test_prewhere_not_supported():
    expected_error = "PREWHERE is not supported if the table is filtered by row-level security"
    assert expected_error in node.query_and_get_error("SELECT * FROM mydb.filtered_table1 PREWHERE 1")
    assert expected_error in node.query_and_get_error("SELECT * FROM mydb.filtered_table2 PREWHERE 1")
    assert expected_error in node.query_and_get_error("SELECT * FROM mydb.filtered_table3 PREWHERE 1")

    # However PREWHERE should still work for user without filtering.
    assert node.query("SELECT * FROM mydb.filtered_table1 PREWHERE 1", user="another") == TSV([[0, 0], [0, 1], [1, 0], [1, 1]])


def test_single_table_name():
    copy_policy_xml('tag_with_table_name.xml')
    assert node.query("SELECT * FROM mydb.table") == TSV([[1, 0], [1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV([[0, 0, 0, 0], [0, 0, 6, 0]])
    assert node.query("SELECT * FROM mydb.filtered_table3") == TSV([[0, 1], [1, 0]])

    assert node.query("SELECT a FROM mydb.table") == TSV([[1], [1]])
    assert node.query("SELECT b FROM mydb.table") == TSV([[0], [1]])
    assert node.query("SELECT a FROM mydb.table WHERE a = 1") == TSV([[1], [1]])
    assert node.query("SELECT a = 1 FROM mydb.table") == TSV([[1], [1]])

    assert node.query("SELECT a FROM mydb.filtered_table3") == TSV([[0], [1]])
    assert node.query("SELECT b FROM mydb.filtered_table3") == TSV([[1], [0]])
    assert node.query("SELECT c FROM mydb.filtered_table3") == TSV([[1], [1]])
    assert node.query("SELECT a + b FROM mydb.filtered_table3") == TSV([[1], [1]])
    assert node.query("SELECT a FROM mydb.filtered_table3 WHERE c = 1") == TSV([[0], [1]])
    assert node.query("SELECT c = 1 FROM mydb.filtered_table3") == TSV([[1], [1]])
    assert node.query("SELECT a + b = 1 FROM mydb.filtered_table3") == TSV([[1], [1]])


def test_policy_from_users_xml_affects_only_user_assigned():
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1,0], [1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table1", user="another") == TSV([[0, 0], [0, 1], [1, 0], [1, 1]])

    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV([[0, 0, 0, 0], [0, 0, 6, 0]])
    assert node.query("SELECT * FROM mydb.filtered_table2", user="another") == TSV([[0, 0, 0, 0], [0, 0, 6, 0], [1, 2, 3, 4], [4, 3, 2, 1]])

    assert node.query("SELECT * FROM mydb.local") == TSV([[1,0], [1, 1], [2, 0], [2, 1]])
    assert node.query("SELECT * FROM mydb.local", user="another") == TSV([[1, 0], [1, 1]])


def test_custom_table_name():
    copy_policy_xml('multiple_tags_with_table_names.xml')
    assert node.query("SELECT * FROM mydb.table") == TSV([[1, 0], [1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV([[0, 0, 0, 0], [0, 0, 6, 0]])
    assert node.query("SELECT * FROM mydb.`.filtered_table4`") == TSV([[0, 1], [1, 0]])

    assert node.query("SELECT a FROM mydb.table") == TSV([[1], [1]])
    assert node.query("SELECT b FROM mydb.table") == TSV([[0], [1]])
    assert node.query("SELECT a FROM mydb.table WHERE a = 1") == TSV([[1], [1]])
    assert node.query("SELECT a = 1 FROM mydb.table") == TSV([[1], [1]])

    assert node.query("SELECT a FROM mydb.`.filtered_table4`") == TSV([[0], [1]])
    assert node.query("SELECT b FROM mydb.`.filtered_table4`") == TSV([[1], [0]])
    assert node.query("SELECT c FROM mydb.`.filtered_table4`") == TSV([[1], [1]])
    assert node.query("SELECT a + b FROM mydb.`.filtered_table4`") == TSV([[1], [1]])
    assert node.query("SELECT a FROM mydb.`.filtered_table4` WHERE c = 1") == TSV([[0], [1]])
    assert node.query("SELECT c = 1 FROM mydb.`.filtered_table4`") == TSV([[1], [1]])
    assert node.query("SELECT a + b = 1 FROM mydb.`.filtered_table4`") == TSV([[1], [1]])


def test_change_of_users_xml_changes_row_policies():
    copy_policy_xml('normal_filters.xml')
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 0], [1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV([[0, 0, 0, 0], [0, 0, 6, 0]])
    assert node.query("SELECT * FROM mydb.filtered_table3") == TSV([[0, 1], [1, 0]])

    copy_policy_xml('all_rows.xml')
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[0, 0], [0, 1], [1, 0], [1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV([[0, 0, 0, 0], [0, 0, 6, 0], [1, 2, 3, 4], [4, 3, 2, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table3") == TSV([[0, 0], [0, 1], [1, 0], [1, 1]])

    copy_policy_xml('no_rows.xml')
    assert node.query("SELECT * FROM mydb.filtered_table1") == ""
    assert node.query("SELECT * FROM mydb.filtered_table2") == ""
    assert node.query("SELECT * FROM mydb.filtered_table3") == ""

    copy_policy_xml('normal_filters.xml')
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 0], [1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV([[0, 0, 0, 0], [0, 0, 6, 0]])
    assert node.query("SELECT * FROM mydb.filtered_table3") == TSV([[0, 1], [1, 0]])

    copy_policy_xml('no_filters.xml')
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[0, 0], [0, 1], [1, 0], [1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV([[0, 0, 0, 0], [0, 0, 6, 0], [1, 2, 3, 4], [4, 3, 2, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table3") == TSV([[0, 0], [0, 1], [1, 0], [1, 1]])

    copy_policy_xml('normal_filters.xml')
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 0], [1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV([[0, 0, 0, 0], [0, 0, 6, 0]])
    assert node.query("SELECT * FROM mydb.filtered_table3") == TSV([[0, 1], [1, 0]])


def test_reload_users_xml_by_timer():
    copy_policy_xml('normal_filters.xml')
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 0], [1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV([[0, 0, 0, 0], [0, 0, 6, 0]])
    assert node.query("SELECT * FROM mydb.filtered_table3") == TSV([[0, 1], [1, 0]])

    time.sleep(1) # The modification time of the 'row_policy.xml' file should be different.
    copy_policy_xml('all_rows.xml', False)
    assert_eq_with_retry(node, "SELECT * FROM mydb.filtered_table1", [[0, 0], [0, 1], [1, 0], [1, 1]])
    assert_eq_with_retry(node, "SELECT * FROM mydb.filtered_table2", [[0, 0, 0, 0], [0, 0, 6, 0], [1, 2, 3, 4], [4, 3, 2, 1]])
    assert_eq_with_retry(node, "SELECT * FROM mydb.filtered_table3", [[0, 0], [0, 1], [1, 0], [1, 1]])

    time.sleep(1) # The modification time of the 'row_policy.xml' file should be different.
    copy_policy_xml('normal_filters.xml', False)
    assert_eq_with_retry(node, "SELECT * FROM mydb.filtered_table1", [[1, 0], [1, 1]])
    assert_eq_with_retry(node, "SELECT * FROM mydb.filtered_table2", [[0, 0, 0, 0], [0, 0, 6, 0]])
    assert_eq_with_retry(node, "SELECT * FROM mydb.filtered_table3", [[0, 1], [1, 0]])


def test_introspection():
    policies = [
        ["another ON mydb.filtered_table1", "another", "mydb", "filtered_table1", "6068883a-0e9d-f802-7e22-0144f8e66d3c", "users.xml", "1",                      0, 0, "['another']", "[]"],
        ["another ON mydb.filtered_table2", "another", "mydb", "filtered_table2", "c019e957-c60b-d54e-cc52-7c90dac5fb01", "users.xml", "1",                      0, 0, "['another']", "[]"],
        ["another ON mydb.filtered_table3", "another", "mydb", "filtered_table3", "4cb080d0-44e8-dbef-6026-346655143628", "users.xml", "1",                      0, 0, "['another']", "[]"],
        ["another ON mydb.local",           "another", "mydb", "local",           "5b23c389-7e18-06bf-a6bc-dd1afbbc0a97", "users.xml", "a = 1",                  0, 0, "['another']", "[]"],
        ["default ON mydb.filtered_table1", "default", "mydb", "filtered_table1", "9e8a8f62-4965-2b5e-8599-57c7b99b3549", "users.xml", "a = 1",                  0, 0, "['default']", "[]"],
        ["default ON mydb.filtered_table2", "default", "mydb", "filtered_table2", "cffae79d-b9bf-a2ef-b798-019c18470b25", "users.xml", "a + b < 1 or c - d > 5", 0, 0, "['default']", "[]"],
        ["default ON mydb.filtered_table3", "default", "mydb", "filtered_table3", "12fc5cef-e3da-3940-ec79-d8be3911f42b", "users.xml", "c = 1",                  0, 0, "['default']", "[]"],
        ["default ON mydb.local",           "default", "mydb", "local",           "cdacaeb5-1d97-f99d-2bb0-4574f290629c", "users.xml", "1",                      0, 0, "['default']", "[]"]
    ]
    assert node.query("SELECT * from system.row_policies ORDER BY short_name, database, table") == TSV(policies)


def test_dcl_introspection():
    assert node.query("SHOW POLICIES") == TSV(["another ON mydb.filtered_table1", "another ON mydb.filtered_table2", "another ON mydb.filtered_table3", "another ON mydb.local", "default ON mydb.filtered_table1", "default ON mydb.filtered_table2", "default ON mydb.filtered_table3", "default ON mydb.local"])
    assert node.query("SHOW POLICIES ON mydb.filtered_table1") == TSV(["another", "default"])
    assert node.query("SHOW POLICIES ON mydb.local") == TSV(["another", "default"])

    assert node.query("SHOW CREATE POLICY default ON mydb.filtered_table1") == "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING a = 1 TO default\n"
    assert node.query("SHOW CREATE POLICY default ON mydb.filtered_table2") == "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING ((a + b) < 1) OR ((c - d) > 5) TO default\n"
    assert node.query("SHOW CREATE POLICY default ON mydb.filtered_table3") == "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING c = 1 TO default\n"
    assert node.query("SHOW CREATE POLICY default ON mydb.local") == "CREATE ROW POLICY default ON mydb.local FOR SELECT USING 1 TO default\n"

    copy_policy_xml('all_rows.xml')
    assert node.query("SHOW POLICIES") == TSV(["another ON mydb.filtered_table1", "another ON mydb.filtered_table2", "another ON mydb.filtered_table3", "default ON mydb.filtered_table1", "default ON mydb.filtered_table2", "default ON mydb.filtered_table3"])
    assert node.query("SHOW CREATE POLICY default ON mydb.filtered_table1") == "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING 1 TO default\n"
    assert node.query("SHOW CREATE POLICY default ON mydb.filtered_table2") == "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING 1 TO default\n"
    assert node.query("SHOW CREATE POLICY default ON mydb.filtered_table3") == "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING 1 TO default\n"

    copy_policy_xml('no_rows.xml')
    assert node.query("SHOW POLICIES") == TSV(["another ON mydb.filtered_table1", "another ON mydb.filtered_table2", "another ON mydb.filtered_table3", "default ON mydb.filtered_table1", "default ON mydb.filtered_table2", "default ON mydb.filtered_table3"])
    assert node.query("SHOW CREATE POLICY default ON mydb.filtered_table1") == "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING NULL TO default\n"
    assert node.query("SHOW CREATE POLICY default ON mydb.filtered_table2") == "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING NULL TO default\n"
    assert node.query("SHOW CREATE POLICY default ON mydb.filtered_table3") == "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING NULL TO default\n"

    copy_policy_xml('no_filters.xml')
    assert node.query("SHOW POLICIES") == ""


def test_dcl_management():
    copy_policy_xml('no_filters.xml')
    assert node.query("SHOW POLICIES") == ""

    node.query("CREATE POLICY pA ON mydb.filtered_table1 FOR SELECT USING a<b")
    assert node.query("SELECT * FROM mydb.filtered_table1") == ""
    assert node.query("SHOW POLICIES ON mydb.filtered_table1") == "pA\n"

    node.query("ALTER POLICY pA ON mydb.filtered_table1 TO default")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[0, 1]])
    assert node.query("SHOW POLICIES ON mydb.filtered_table1") == "pA\n"

    node.query("ALTER POLICY pA ON mydb.filtered_table1 FOR SELECT USING a>b")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 0]])

    node.query("ALTER POLICY pA ON mydb.filtered_table1 RENAME TO pB")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 0]])
    assert node.query("SHOW POLICIES ON mydb.filtered_table1") == "pB\n"
    assert node.query("SHOW CREATE POLICY pB ON mydb.filtered_table1") == "CREATE ROW POLICY pB ON mydb.filtered_table1 FOR SELECT USING a > b TO default\n"

    node.query("DROP POLICY pB ON mydb.filtered_table1")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[0, 0], [0, 1], [1, 0], [1, 1]])
    assert node.query("SHOW POLICIES") == ""


def test_users_xml_is_readonly():
    assert re.search("storage is readonly", node.query_and_get_error("DROP POLICY default ON mydb.filtered_table1"))


def test_miscellaneous_engines():
    copy_policy_xml('normal_filters.xml')

    # ReplicatedMergeTree
    node.query("DROP TABLE mydb.filtered_table1")
    node.query("CREATE TABLE mydb.filtered_table1 (a UInt8, b UInt8) ENGINE ReplicatedMergeTree('/clickhouse/tables/00-00/filtered_table1', 'replica1') ORDER BY a")
    node.query("INSERT INTO mydb.filtered_table1 values (0, 0), (0, 1), (1, 0), (1, 1)")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 0], [1, 1]])

    # CollapsingMergeTree
    node.query("DROP TABLE mydb.filtered_table1")
    node.query("CREATE TABLE mydb.filtered_table1 (a UInt8, b Int8) ENGINE CollapsingMergeTree(b) ORDER BY a")
    node.query("INSERT INTO mydb.filtered_table1 values (0, 1), (0, 1), (1, 1), (1, 1)")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 1], [1, 1]])

    # ReplicatedCollapsingMergeTree
    node.query("DROP TABLE mydb.filtered_table1")
    node.query("CREATE TABLE mydb.filtered_table1 (a UInt8, b Int8) ENGINE ReplicatedCollapsingMergeTree('/clickhouse/tables/00-00/filtered_table1', 'replica1', b) ORDER BY a")
    node.query("INSERT INTO mydb.filtered_table1 values (0, 1), (0, 1), (1, 1), (1, 1)")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 1], [1, 1]])

    # DistributedMergeTree
    node.query("DROP TABLE IF EXISTS mydb.not_filtered_table")
    node.query("CREATE TABLE mydb.not_filtered_table (a UInt8, b UInt8) ENGINE Distributed('test_local_cluster', mydb, local)")
    assert node.query("SELECT * FROM mydb.not_filtered_table", user="another") == TSV([[1, 0], [1, 1], [1, 0], [1, 1]])
    assert node.query("SELECT sum(a), b FROM mydb.not_filtered_table GROUP BY b ORDER BY b", user="another") == TSV([[2, 0], [2, 1]])
