import os
import re
import time

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/remote_servers.xml"],
    user_configs=[
        "configs/users.d/row_policy.xml",
        "configs/users.d/another_user.xml",
        "configs/users.d/any_join_distinct_right_table_keys.xml",
    ],
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.d/remote_servers.xml"],
    user_configs=[
        "configs/users.d/row_policy.xml",
        "configs/users.d/another_user.xml",
        "configs/users.d/any_join_distinct_right_table_keys.xml",
    ],
    with_zookeeper=True,
)
nodes = [node, node2]


def copy_policy_xml(local_file_name, reload_immediately=True):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    for current_node in nodes:
        current_node.copy_file_to_container(
            os.path.join(script_dir, local_file_name),
            "/etc/clickhouse-server/users.d/row_policy.xml",
        )
        if reload_immediately:
            current_node.query("SYSTEM RELOAD CONFIG")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        for current_node in nodes:
            current_node.query(
                """
                CREATE DATABASE mydb;

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
            """
            )

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
        copy_policy_xml("normal_filters.xml")
        for current_node in nodes:
            current_node.query("DROP POLICY IF EXISTS pA, pB ON mydb.filtered_table1")
            current_node.query("DROP POLICY IF EXISTS pC ON mydb.other_table")
            current_node.query("DROP POLICY IF EXISTS all_data ON dist_tbl, local_tbl")
            current_node.query(
                "DROP POLICY IF EXISTS role1_data ON dist_tbl, local_tbl"
            )


def test_smoke():
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 0], [1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV(
        [[0, 0, 0, 0], [0, 0, 6, 0]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table3") == TSV([[0, 1], [1, 0]])

    assert node.query("SELECT a FROM mydb.filtered_table1") == TSV([[1], [1]])
    assert node.query("SELECT b FROM mydb.filtered_table1") == TSV([[0], [1]])
    assert node.query("SELECT a FROM mydb.filtered_table1 WHERE a = 1") == TSV(
        [[1], [1]]
    )
    assert node.query("SELECT a FROM mydb.filtered_table1 WHERE a IN (1)") == TSV(
        [[1], [1]]
    )
    assert node.query("SELECT a = 1 FROM mydb.filtered_table1") == TSV([[1], [1]])

    assert node.query("SELECT a FROM mydb.filtered_table3") == TSV([[0], [1]])
    assert node.query("SELECT b FROM mydb.filtered_table3") == TSV([[1], [0]])
    assert node.query("SELECT c FROM mydb.filtered_table3") == TSV([[1], [1]])
    assert node.query("SELECT a + b FROM mydb.filtered_table3") == TSV([[1], [1]])
    assert node.query("SELECT a FROM mydb.filtered_table3 WHERE c = 1") == TSV(
        [[0], [1]]
    )
    assert node.query("SELECT c = 1 FROM mydb.filtered_table3") == TSV([[1], [1]])
    assert node.query("SELECT a + b = 1 FROM mydb.filtered_table3") == TSV([[1], [1]])


def test_join():
    assert node.query(
        "SELECT * FROM mydb.filtered_table1 as t1 ANY LEFT JOIN mydb.filtered_table1 as t2 ON t1.a = t2.b"
    ) == TSV([[1, 0, 1, 1], [1, 1, 1, 1]])
    assert node.query(
        "SELECT * FROM mydb.filtered_table1 as t2 ANY RIGHT JOIN mydb.filtered_table1 as t1 ON t2.b = t1.a"
    ) == TSV([[1, 1, 1, 0]])


def test_cannot_trick_row_policy_with_keyword_with():
    assert node.query("WITH 0 AS a SELECT a FROM mydb.filtered_table1") == TSV(
        [[0], [0]]
    )
    assert node.query("WITH 0 AS a SELECT b FROM mydb.filtered_table1") == TSV(
        [[0], [1]]
    )

    assert node.query("WITH 0 AS a SELECT * FROM mydb.filtered_table1") == TSV(
        [[1, 0], [1, 1]]
    )
    assert node.query(
        "WITH 0 AS a SELECT * FROM mydb.filtered_table1 WHERE a >= 0 AND b >= 0 SETTINGS optimize_move_to_prewhere = 0"
    ) == TSV([[1, 0], [1, 1]])
    assert node.query(
        "WITH 0 AS a SELECT * FROM mydb.filtered_table1 PREWHERE a >= 0 AND b >= 0"
    ) == TSV([[1, 0], [1, 1]])
    assert node.query(
        "WITH 0 AS a SELECT * FROM mydb.filtered_table1 PREWHERE a >= 0 WHERE b >= 0"
    ) == TSV([[1, 0], [1, 1]])
    assert node.query(
        "WITH 0 AS a SELECT * FROM mydb.filtered_table1 PREWHERE b >= 0 WHERE a >= 0"
    ) == TSV([[1, 0], [1, 1]])

    assert node.query("WITH 0 AS a SELECT a, b FROM mydb.filtered_table1") == TSV(
        [[0, 0], [0, 1]]
    )
    assert node.query(
        "WITH 0 AS a SELECT a, b FROM mydb.filtered_table1 WHERE a >= 0 AND b >= 0 SETTINGS optimize_move_to_prewhere = 0"
    ) == TSV([[0, 0], [0, 1]])
    assert node.query(
        "WITH 0 AS a SELECT a, b FROM mydb.filtered_table1 PREWHERE a >= 0 AND b >= 0"
    ) == TSV([[0, 0], [0, 1]])
    assert node.query(
        "WITH 0 AS a SELECT a, b FROM mydb.filtered_table1 PREWHERE a >= 0 WHERE b >= 0"
    ) == TSV([[0, 0], [0, 1]])
    assert node.query(
        "WITH 0 AS a SELECT a, b FROM mydb.filtered_table1 PREWHERE b >= 0 WHERE a >= 0"
    ) == TSV([[0, 0], [0, 1]])

    assert node.query("WITH 0 AS c SELECT * FROM mydb.filtered_table3") == TSV(
        [[0, 1], [1, 0]]
    )
    assert node.query(
        "WITH 0 AS c SELECT * FROM mydb.filtered_table3 WHERE c >= 0 AND a >= 0 SETTINGS optimize_move_to_prewhere = 0"
    ) == TSV([[0, 1], [1, 0]])
    assert node.query(
        "WITH 0 AS c SELECT * FROM mydb.filtered_table3 PREWHERE c >= 0 AND a >= 0"
    ) == TSV([[0, 1], [1, 0]])
    assert node.query(
        "WITH 0 AS c SELECT * FROM mydb.filtered_table3 PREWHERE c >= 0 WHERE a >= 0"
    ) == TSV([[0, 1], [1, 0]])
    assert node.query(
        "WITH 0 AS c SELECT * FROM mydb.filtered_table3 PREWHERE a >= 0 WHERE c >= 0"
    ) == TSV([[0, 1], [1, 0]])

    assert node.query("WITH 0 AS c SELECT a, b, c FROM mydb.filtered_table3") == TSV(
        [[0, 1, 0], [1, 0, 0]]
    )
    assert node.query(
        "WITH 0 AS c SELECT a, b, c FROM mydb.filtered_table3 WHERE c >= 0 AND a >= 0 SETTINGS optimize_move_to_prewhere = 0"
    ) == TSV([[0, 1, 0], [1, 0, 0]])
    assert node.query(
        "WITH 0 AS c SELECT a, b, c FROM mydb.filtered_table3 PREWHERE c >= 0 AND a >= 0"
    ) == TSV([[0, 1, 0], [1, 0, 0]])
    assert node.query(
        "WITH 0 AS c SELECT a, b, c FROM mydb.filtered_table3 PREWHERE c >= 0 WHERE a >= 0"
    ) == TSV([[0, 1, 0], [1, 0, 0]])
    assert node.query(
        "WITH 0 AS c SELECT a, b, c FROM mydb.filtered_table3 PREWHERE a >= 0 WHERE c >= 0"
    ) == TSV([[0, 1, 0], [1, 0, 0]])


def test_policy_from_users_xml_affects_only_user_assigned():
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 0], [1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table1", user="another") == TSV(
        [[0, 0], [0, 1], [1, 0], [1, 1]]
    )

    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV(
        [[0, 0, 0, 0], [0, 0, 6, 0]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table2", user="another") == TSV(
        [[0, 0, 0, 0], [0, 0, 6, 0], [1, 2, 3, 4], [4, 3, 2, 1]]
    )

    assert node.query("SELECT * FROM mydb.local") == TSV(
        [[1, 0], [1, 1], [2, 0], [2, 1]]
    )
    assert node.query("SELECT * FROM mydb.local", user="another") == TSV(
        [[1, 0], [1, 1]]
    )


def test_with_prewhere():
    copy_policy_xml("normal_filter2_table2.xml")
    assert node.query(
        "SELECT * FROM mydb.filtered_table2 WHERE a > 1 SETTINGS optimize_move_to_prewhere = 0"
    ) == TSV([[4, 3, 2, 1]])
    assert node.query(
        "SELECT a FROM mydb.filtered_table2 WHERE a > 1 SETTINGS optimize_move_to_prewhere = 0"
    ) == TSV([[4]])
    assert node.query(
        "SELECT a, b FROM mydb.filtered_table2 WHERE a > 1 SETTINGS optimize_move_to_prewhere = 0"
    ) == TSV([[4, 3]])
    assert node.query(
        "SELECT b, c FROM mydb.filtered_table2 WHERE a > 1 SETTINGS optimize_move_to_prewhere = 0"
    ) == TSV([[3, 2]])
    assert node.query(
        "SELECT d FROM mydb.filtered_table2 WHERE a > 1 SETTINGS optimize_move_to_prewhere = 0"
    ) == TSV([[1]])

    assert node.query("SELECT * FROM mydb.filtered_table2 PREWHERE a > 1") == TSV(
        [[4, 3, 2, 1]]
    )
    assert node.query("SELECT a FROM mydb.filtered_table2 PREWHERE a > 1") == TSV([[4]])
    assert node.query("SELECT a, b FROM mydb.filtered_table2 PREWHERE a > 1") == TSV(
        [[4, 3]]
    )
    assert node.query("SELECT b, c FROM mydb.filtered_table2 PREWHERE a > 1") == TSV(
        [[3, 2]]
    )
    assert node.query("SELECT d FROM mydb.filtered_table2 PREWHERE a > 1") == TSV([[1]])

    assert node.query(
        "SELECT * FROM mydb.filtered_table2 PREWHERE a < 4 WHERE b < 10"
    ) == TSV([[1, 2, 3, 4]])
    assert node.query(
        "SELECT a FROM mydb.filtered_table2 PREWHERE a < 4 WHERE b < 10"
    ) == TSV([[1]])
    assert node.query(
        "SELECT b FROM mydb.filtered_table2 PREWHERE a < 4 WHERE b < 10"
    ) == TSV([[2]])
    assert node.query(
        "SELECT a, b FROM mydb.filtered_table2 PREWHERE a < 4 WHERE b < 10"
    ) == TSV([[1, 2]])
    assert node.query(
        "SELECT a, c FROM mydb.filtered_table2 PREWHERE a < 4 WHERE b < 10"
    ) == TSV([[1, 3]])
    assert node.query(
        "SELECT b, d FROM mydb.filtered_table2 PREWHERE a < 4 WHERE b < 10"
    ) == TSV([[2, 4]])
    assert node.query(
        "SELECT c, d FROM mydb.filtered_table2 PREWHERE a < 4 WHERE b < 10"
    ) == TSV([[3, 4]])


def test_throwif_error_in_where_with_same_condition_as_filter():
    copy_policy_xml("normal_filter2_table2.xml")
    assert "expected" in node.query_and_get_error(
        "SELECT * FROM mydb.filtered_table2 WHERE throwIf(a > 0, 'expected') = 0 SETTINGS optimize_move_to_prewhere = 0"
    )


def test_throwif_error_in_prewhere_with_same_condition_as_filter():
    copy_policy_xml("normal_filter2_table2.xml")
    assert "expected" in node.query_and_get_error(
        "SELECT * FROM mydb.filtered_table2 PREWHERE throwIf(a > 0, 'expected') = 0"
    )


def test_throwif_in_where_doesnt_expose_restricted_data():
    copy_policy_xml("no_filters.xml")
    assert "expected" in node.query_and_get_error(
        "SELECT * FROM mydb.filtered_table2 WHERE throwIf(a = 0, 'expected') = 0 SETTINGS optimize_move_to_prewhere = 0"
    )

    copy_policy_xml("normal_filter2_table2.xml")
    assert node.query(
        "SELECT * FROM mydb.filtered_table2 WHERE throwIf(a = 0, 'pwned') = 0 SETTINGS optimize_move_to_prewhere = 0"
    ) == TSV([[1, 2, 3, 4], [4, 3, 2, 1]])


def test_throwif_in_prewhere_doesnt_expose_restricted_data():
    copy_policy_xml("no_filters.xml")
    assert "expected" in node.query_and_get_error(
        "SELECT * FROM mydb.filtered_table2 PREWHERE throwIf(a = 0, 'expected') = 0"
    )

    copy_policy_xml("normal_filter2_table2.xml")
    assert node.query(
        "SELECT * FROM mydb.filtered_table2 PREWHERE throwIf(a = 0, 'pwned') = 0"
    ) == TSV([[1, 2, 3, 4], [4, 3, 2, 1]])


def test_change_of_users_xml_changes_row_policies():
    copy_policy_xml("normal_filters.xml")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 0], [1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV(
        [[0, 0, 0, 0], [0, 0, 6, 0]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table3") == TSV([[0, 1], [1, 0]])

    copy_policy_xml("all_rows.xml")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV(
        [[0, 0], [0, 1], [1, 0], [1, 1]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV(
        [[0, 0, 0, 0], [0, 0, 6, 0], [1, 2, 3, 4], [4, 3, 2, 1]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table3") == TSV(
        [[0, 0], [0, 1], [1, 0], [1, 1]]
    )

    copy_policy_xml("no_rows.xml")
    assert node.query("SELECT * FROM mydb.filtered_table1") == ""
    assert node.query("SELECT * FROM mydb.filtered_table2") == ""
    assert node.query("SELECT * FROM mydb.filtered_table3") == ""

    copy_policy_xml("normal_filters.xml")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 0], [1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV(
        [[0, 0, 0, 0], [0, 0, 6, 0]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table3") == TSV([[0, 1], [1, 0]])

    copy_policy_xml("normal_filter2_table2.xml")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV(
        [[0, 0], [0, 1], [1, 0], [1, 1]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV(
        [[1, 2, 3, 4], [4, 3, 2, 1]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table3") == TSV(
        [[0, 0], [0, 1], [1, 0], [1, 1]]
    )

    copy_policy_xml("no_filters.xml")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV(
        [[0, 0], [0, 1], [1, 0], [1, 1]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV(
        [[0, 0, 0, 0], [0, 0, 6, 0], [1, 2, 3, 4], [4, 3, 2, 1]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table3") == TSV(
        [[0, 0], [0, 1], [1, 0], [1, 1]]
    )

    copy_policy_xml("normal_filters.xml")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 0], [1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV(
        [[0, 0, 0, 0], [0, 0, 6, 0]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table3") == TSV([[0, 1], [1, 0]])


def test_reload_users_xml_by_timer():
    copy_policy_xml("normal_filters.xml")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 0], [1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV(
        [[0, 0, 0, 0], [0, 0, 6, 0]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table3") == TSV([[0, 1], [1, 0]])

    time.sleep(
        1
    )  # The modification time of the 'row_policy.xml' file should be different.
    copy_policy_xml("all_rows.xml", False)
    assert_eq_with_retry(
        node, "SELECT * FROM mydb.filtered_table1", [[0, 0], [0, 1], [1, 0], [1, 1]]
    )
    assert_eq_with_retry(
        node,
        "SELECT * FROM mydb.filtered_table2",
        [[0, 0, 0, 0], [0, 0, 6, 0], [1, 2, 3, 4], [4, 3, 2, 1]],
    )
    assert_eq_with_retry(
        node, "SELECT * FROM mydb.filtered_table3", [[0, 0], [0, 1], [1, 0], [1, 1]]
    )

    time.sleep(
        1
    )  # The modification time of the 'row_policy.xml' file should be different.
    copy_policy_xml("normal_filters.xml", False)
    assert_eq_with_retry(node, "SELECT * FROM mydb.filtered_table1", [[1, 0], [1, 1]])
    assert_eq_with_retry(
        node, "SELECT * FROM mydb.filtered_table2", [[0, 0, 0, 0], [0, 0, 6, 0]]
    )
    assert_eq_with_retry(node, "SELECT * FROM mydb.filtered_table3", [[0, 1], [1, 0]])


def test_introspection():
    policies = [
        [
            "another ON mydb.local",
            "another",
            "mydb",
            "local",
            "5b23c389-7e18-06bf-a6bc-dd1afbbc0a97",
            "users.xml",
            "a = 1",
            0,
            0,
            "['another']",
            "[]",
        ],
        [
            "default ON mydb.filtered_table1",
            "default",
            "mydb",
            "filtered_table1",
            "9e8a8f62-4965-2b5e-8599-57c7b99b3549",
            "users.xml",
            "a = 1",
            0,
            0,
            "['default']",
            "[]",
        ],
        [
            "default ON mydb.filtered_table2",
            "default",
            "mydb",
            "filtered_table2",
            "cffae79d-b9bf-a2ef-b798-019c18470b25",
            "users.xml",
            "a + b < 1 or c - d > 5",
            0,
            0,
            "['default']",
            "[]",
        ],
        [
            "default ON mydb.filtered_table3",
            "default",
            "mydb",
            "filtered_table3",
            "12fc5cef-e3da-3940-ec79-d8be3911f42b",
            "users.xml",
            "c = 1",
            0,
            0,
            "['default']",
            "[]",
        ],
    ]
    assert node.query(
        "SELECT * from system.row_policies ORDER BY short_name, database, table"
    ) == TSV(policies)


def test_dcl_introspection():
    assert node.query("SHOW POLICIES") == TSV(
        [
            "another ON mydb.local",
            "default ON mydb.filtered_table1",
            "default ON mydb.filtered_table2",
            "default ON mydb.filtered_table3",
        ]
    )

    assert node.query("SHOW POLICIES ON mydb.filtered_table1") == TSV(["default"])
    assert node.query("SHOW POLICIES ON mydb.local") == TSV(["another"])
    assert node.query("SHOW POLICIES ON mydb.*") == TSV(
        [
            "another ON mydb.local",
            "default ON mydb.filtered_table1",
            "default ON mydb.filtered_table2",
            "default ON mydb.filtered_table3",
        ]
    )
    assert node.query("SHOW POLICIES default") == TSV(
        [
            "default ON mydb.filtered_table1",
            "default ON mydb.filtered_table2",
            "default ON mydb.filtered_table3",
        ]
    )

    assert (
        node.query("SHOW CREATE POLICY default ON mydb.filtered_table1")
        == "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING a = 1 TO default\n"
    )
    assert (
        node.query("SHOW CREATE POLICY default ON mydb.filtered_table2")
        == "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING ((a + b) < 1) OR ((c - d) > 5) TO default\n"
    )
    assert (
        node.query("SHOW CREATE POLICY default ON mydb.filtered_table3")
        == "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING c = 1 TO default\n"
    )
    assert "no row policy" in node.query_and_get_error(
        "SHOW CREATE POLICY default ON mydb.local"
    )

    assert node.query("SHOW CREATE POLICY default") == TSV(
        [
            "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING a = 1 TO default",
            "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING ((a + b) < 1) OR ((c - d) > 5) TO default",
            "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING c = 1 TO default",
        ]
    )
    assert node.query("SHOW CREATE POLICIES ON mydb.filtered_table1") == TSV(
        [
            "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING a = 1 TO default",
        ]
    )
    assert node.query("SHOW CREATE POLICIES ON mydb.*") == TSV(
        [
            "CREATE ROW POLICY another ON mydb.local FOR SELECT USING a = 1 TO another",
            "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING a = 1 TO default",
            "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING ((a + b) < 1) OR ((c - d) > 5) TO default",
            "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING c = 1 TO default",
        ]
    )
    assert node.query("SHOW CREATE POLICIES") == TSV(
        [
            "CREATE ROW POLICY another ON mydb.local FOR SELECT USING a = 1 TO another",
            "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING a = 1 TO default",
            "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING ((a + b) < 1) OR ((c - d) > 5) TO default",
            "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING c = 1 TO default",
        ]
    )

    expected_access = (
        "CREATE ROW POLICY another ON mydb.local FOR SELECT USING a = 1 TO another\n"
        "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING a = 1 TO default\n"
        "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING ((a + b) < 1) OR ((c - d) > 5) TO default\n"
        "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING c = 1 TO default\n"
    )
    assert expected_access in node.query("SHOW ACCESS")

    copy_policy_xml("all_rows.xml")
    assert node.query("SHOW POLICIES") == TSV(
        [
            "default ON mydb.filtered_table1",
            "default ON mydb.filtered_table2",
            "default ON mydb.filtered_table3",
        ]
    )
    assert (
        node.query("SHOW CREATE POLICY default ON mydb.filtered_table1")
        == "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING 1 TO default\n"
    )
    assert (
        node.query("SHOW CREATE POLICY default ON mydb.filtered_table2")
        == "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING 1 TO default\n"
    )
    assert (
        node.query("SHOW CREATE POLICY default ON mydb.filtered_table3")
        == "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING 1 TO default\n"
    )

    copy_policy_xml("no_rows.xml")
    assert node.query("SHOW POLICIES") == TSV(
        [
            "default ON mydb.filtered_table1",
            "default ON mydb.filtered_table2",
            "default ON mydb.filtered_table3",
        ]
    )
    assert (
        node.query("SHOW CREATE POLICY default ON mydb.filtered_table1")
        == "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING NULL TO default\n"
    )
    assert (
        node.query("SHOW CREATE POLICY default ON mydb.filtered_table2")
        == "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING NULL TO default\n"
    )
    assert (
        node.query("SHOW CREATE POLICY default ON mydb.filtered_table3")
        == "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING NULL TO default\n"
    )

    copy_policy_xml("no_filters.xml")
    assert node.query("SHOW POLICIES") == ""


def test_dcl_management():
    copy_policy_xml("no_filters.xml")
    assert node.query("SHOW POLICIES") == ""

    node.query("CREATE POLICY pA ON mydb.filtered_table1 FOR SELECT USING a<b")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV(
        [[0, 0], [0, 1], [1, 0], [1, 1]]
    )
    assert node.query("SHOW POLICIES ON mydb.filtered_table1") == "pA\n"

    node.query("ALTER POLICY pA ON mydb.filtered_table1 TO default")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[0, 1]])
    assert node.query("SHOW POLICIES ON mydb.filtered_table1") == "pA\n"

    node.query("ALTER POLICY pA ON mydb.filtered_table1 FOR SELECT USING a>b")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 0]])

    node.query("ALTER POLICY pA ON mydb.filtered_table1 RENAME TO pB")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 0]])
    assert node.query("SHOW POLICIES ON mydb.filtered_table1") == "pB\n"
    assert (
        node.query("SHOW CREATE POLICY pB ON mydb.filtered_table1")
        == "CREATE ROW POLICY pB ON mydb.filtered_table1 FOR SELECT USING a > b TO default\n"
    )

    node.query("DROP POLICY pB ON mydb.filtered_table1")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV(
        [[0, 0], [0, 1], [1, 0], [1, 1]]
    )
    assert node.query("SHOW POLICIES") == ""


def test_grant_create_row_policy():
    copy_policy_xml("no_filters.xml")
    assert node.query("SHOW POLICIES") == ""
    node.query("CREATE USER X")

    expected_error = "necessary to have grant CREATE ROW POLICY ON mydb.filtered_table1"
    assert expected_error in node.query_and_get_error(
        "CREATE POLICY pA ON mydb.filtered_table1 FOR SELECT USING a<b", user="X"
    )
    node.query("GRANT CREATE POLICY ON mydb.filtered_table1 TO X")
    node.query(
        "CREATE POLICY pA ON mydb.filtered_table1 FOR SELECT USING a<b", user="X"
    )
    expected_error = "necessary to have grant CREATE ROW POLICY ON mydb.filtered_table2"
    assert expected_error in node.query_and_get_error(
        "CREATE POLICY pA ON mydb.filtered_table2 FOR SELECT USING a<b", user="X"
    )

    expected_error = "necessary to have grant ALTER ROW POLICY ON mydb.filtered_table1"
    assert expected_error in node.query_and_get_error(
        "ALTER POLICY pA ON mydb.filtered_table1 FOR SELECT USING a==b", user="X"
    )
    node.query("GRANT ALTER POLICY ON mydb.filtered_table1 TO X")
    node.query(
        "ALTER POLICY pA ON mydb.filtered_table1 FOR SELECT USING a==b", user="X"
    )
    expected_error = "necessary to have grant ALTER ROW POLICY ON mydb.filtered_table2"
    assert expected_error in node.query_and_get_error(
        "ALTER POLICY pA ON mydb.filtered_table2 FOR SELECT USING a==b", user="X"
    )

    expected_error = "necessary to have grant DROP ROW POLICY ON mydb.filtered_table1"
    assert expected_error in node.query_and_get_error(
        "DROP POLICY pA ON mydb.filtered_table1", user="X"
    )
    node.query("GRANT DROP POLICY ON mydb.filtered_table1 TO X")
    node.query("DROP POLICY pA ON mydb.filtered_table1", user="X")
    expected_error = "necessary to have grant DROP ROW POLICY ON mydb.filtered_table2"
    assert expected_error in node.query_and_get_error(
        "DROP POLICY pA ON mydb.filtered_table2", user="X"
    )

    node.query("REVOKE ALL ON *.* FROM X")

    expected_error = "necessary to have grant CREATE ROW POLICY ON mydb.filtered_table1"
    assert expected_error in node.query_and_get_error(
        "CREATE POLICY pA ON mydb.filtered_table1 FOR SELECT USING a<b", user="X"
    )
    node.query("GRANT CREATE POLICY ON *.* TO X")
    node.query(
        "CREATE POLICY pA ON mydb.filtered_table1 FOR SELECT USING a<b", user="X"
    )

    expected_error = "necessary to have grant ALTER ROW POLICY ON mydb.filtered_table1"
    assert expected_error in node.query_and_get_error(
        "ALTER POLICY pA ON mydb.filtered_table1 FOR SELECT USING a==b", user="X"
    )
    node.query("GRANT ALTER POLICY ON *.* TO X")
    node.query(
        "ALTER POLICY pA ON mydb.filtered_table1 FOR SELECT USING a==b", user="X"
    )

    expected_error = "necessary to have grant DROP ROW POLICY ON mydb.filtered_table1"
    assert expected_error in node.query_and_get_error(
        "DROP POLICY pA ON mydb.filtered_table1", user="X"
    )
    node.query("GRANT DROP POLICY ON *.* TO X")
    node.query("DROP POLICY pA ON mydb.filtered_table1", user="X")

    node.query("DROP USER X")


def test_users_xml_is_readonly():
    assert re.search(
        "storage is readonly",
        node.query_and_get_error("DROP POLICY default ON mydb.filtered_table1"),
    )


def test_dcl_users_with_policies_from_users_xml():
    node.query("CREATE USER X")
    node.query("GRANT SELECT ON mydb.filtered_table1 TO X")

    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 0], [1, 1]])

    assert node.query("SELECT * FROM mydb.filtered_table1", user="X") == TSV(
        [[0, 0], [0, 1], [1, 0], [1, 1]]
    )

    node.query("DROP USER X")


def test_some_users_without_policies():
    copy_policy_xml("no_filters.xml")
    assert node.query("SHOW POLICIES") == ""
    node.query("CREATE USER X, Y")
    node.query("GRANT SELECT ON mydb.filtered_table1 TO X, Y")

    # permissive a >= b for X, none for Y
    node.query(
        "CREATE POLICY pA ON mydb.filtered_table1 FOR SELECT USING a >= b AS permissive TO X"
    )
    assert node.query("SELECT * FROM mydb.filtered_table1", user="X") == TSV(
        [[0, 0], [1, 0], [1, 1]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table1", user="Y") == TSV(
        [[0, 0], [0, 1], [1, 0], [1, 1]]
    )

    # restrictive a >=b for X, none for Y
    node.query("ALTER POLICY pA ON mydb.filtered_table1 AS restrictive")
    assert node.query("SELECT * FROM mydb.filtered_table1", user="X") == TSV(
        [[0, 0], [1, 0], [1, 1]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table1", user="Y") == TSV(
        [[0, 0], [0, 1], [1, 0], [1, 1]]
    )

    # permissive a >= b for X, restrictive a <= b for X, none for Y
    node.query("ALTER POLICY pA ON mydb.filtered_table1 AS permissive")
    node.query(
        "CREATE POLICY pB ON mydb.filtered_table1 FOR SELECT USING a <= b AS restrictive TO X"
    )
    assert node.query("SELECT * FROM mydb.filtered_table1", user="X") == TSV(
        [[0, 0], [1, 1]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table1", user="Y") == TSV(
        [[0, 0], [0, 1], [1, 0], [1, 1]]
    )

    # permissive a >= b for X, restrictive a <= b for Y
    node.query("ALTER POLICY pB ON mydb.filtered_table1 TO Y")
    assert node.query("SELECT * FROM mydb.filtered_table1", user="X") == TSV(
        [[0, 0], [1, 0], [1, 1]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table1", user="Y") == TSV(
        [[0, 0], [0, 1], [1, 1]]
    )

    node.query("DROP POLICY pA, pB ON mydb.filtered_table1")
    node.query("DROP USER X, Y")


def test_tags_with_db_and_table_names():
    copy_policy_xml("tags_with_db_and_table_names.xml")

    assert node.query("SELECT * FROM mydb.table") == TSV([[0, 0], [0, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table2") == TSV([[0, 0, 6, 0]])
    assert node.query("SELECT * FROM mydb.filtered_table3") == TSV([[0, 0]])
    assert node.query("SELECT * FROM mydb.`.filtered_table4`") == TSV([[1, 1]])

    assert node.query("SHOW CREATE POLICIES default") == TSV(
        [
            "CREATE ROW POLICY default ON mydb.`.filtered_table4` FOR SELECT USING c = 2 TO default",
            "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING c > (d + 5) TO default",
            "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING c = 0 TO default",
            "CREATE ROW POLICY default ON mydb.table FOR SELECT USING a = 0 TO default",
        ]
    )


def test_miscellaneous_engines():
    node.query(
        "CREATE ROW POLICY OR REPLACE pC ON mydb.other_table FOR SELECT USING a = 1 TO default"
    )
    assert node.query("SHOW ROW POLICIES ON mydb.other_table") == "pC\n"

    # ReplicatedMergeTree
    node.query("DROP TABLE IF EXISTS mydb.other_table")
    node.query(
        "CREATE TABLE mydb.other_table (a UInt8, b UInt8) ENGINE ReplicatedMergeTree('/clickhouse/tables/00-00/filtered_table1', 'replica1') ORDER BY a"
    )
    node.query("INSERT INTO mydb.other_table values (0, 0), (0, 1), (1, 0), (1, 1)")
    assert node.query("SELECT * FROM mydb.other_table") == TSV([[1, 0], [1, 1]])

    # CollapsingMergeTree
    node.query("DROP TABLE mydb.other_table")
    node.query(
        "CREATE TABLE mydb.other_table (a UInt8, b Int8) ENGINE CollapsingMergeTree(b) ORDER BY a"
    )
    node.query("INSERT INTO mydb.other_table values (0, 1), (0, 1), (1, 1), (1, 1)")
    assert node.query("SELECT * FROM mydb.other_table") == TSV([[1, 1], [1, 1]])

    # ReplicatedCollapsingMergeTree
    node.query("DROP TABLE mydb.other_table")
    node.query(
        "CREATE TABLE mydb.other_table (a UInt8, b Int8) ENGINE ReplicatedCollapsingMergeTree('/clickhouse/tables/00-01/filtered_table1', 'replica1', b) ORDER BY a"
    )
    node.query("INSERT INTO mydb.other_table values (0, 1), (0, 1), (1, 1), (1, 1)")
    assert node.query("SELECT * FROM mydb.other_table") == TSV([[1, 1], [1, 1]])

    node.query("DROP ROW POLICY pC ON mydb.other_table")

    # DistributedMergeTree
    node.query("DROP TABLE IF EXISTS mydb.other_table")
    node.query(
        "CREATE TABLE mydb.other_table (a UInt8, b UInt8) ENGINE Distributed('test_local_cluster', mydb, local)"
    )
    assert node.query("SELECT * FROM mydb.other_table", user="another") == TSV(
        [[1, 0], [1, 1], [1, 0], [1, 1]]
    )
    assert node.query(
        "SELECT sum(a), b FROM mydb.other_table GROUP BY b ORDER BY b", user="another"
    ) == TSV([[2, 0], [2, 1]])


def test_policy_on_distributed_table_via_role():
    node.query("DROP TABLE IF EXISTS local_tbl")
    node.query("DROP TABLE IF EXISTS dist_tbl")

    node.query(
        "CREATE TABLE local_tbl engine=MergeTree ORDER BY tuple() as select * FROM numbers(10)"
    )
    node.query(
        "CREATE TABLE dist_tbl ENGINE=Distributed( 'test_cluster_two_shards_localhost', default, local_tbl) AS local_tbl"
    )

    node.query("CREATE ROLE OR REPLACE 'role1'")
    node.query("CREATE USER OR REPLACE 'user1' DEFAULT ROLE 'role1'")

    node.query("GRANT SELECT ON dist_tbl TO 'role1'")
    node.query("GRANT SELECT ON local_tbl TO 'role1'")

    node.query(
        "CREATE ROW POLICY OR REPLACE 'all_data' ON dist_tbl, local_tbl USING 1 TO ALL EXCEPT 'role1'"
    )
    node.query(
        "CREATE ROW POLICY OR REPLACE 'role1_data' ON dist_tbl, local_tbl USING number % 2 = 0 TO 'role1'"
    )

    assert node.query(
        "SELECT * FROM local_tbl SETTINGS prefer_localhost_replica=0", user="user1"
    ) == TSV([[0], [2], [4], [6], [8]])
    assert node.query(
        "SELECT * FROM dist_tbl SETTINGS prefer_localhost_replica=0", user="user1"
    ) == TSV([[0], [2], [4], [6], [8], [0], [2], [4], [6], [8]])
