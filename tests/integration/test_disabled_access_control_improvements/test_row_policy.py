import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/disable_access_control_improvements.xml"],
    user_configs=[
        "configs/users.d/row_policy.xml",
        "configs/users.d/another_user.xml",
    ],
)


def copy_policy_xml(local_file_name):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    node.copy_file_to_container(
        os.path.join(script_dir, local_file_name),
        "/etc/clickhouse-server/users.d/row_policy.xml",
    )
    node.query("SYSTEM RELOAD CONFIG")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        node.query(
            """
            CREATE DATABASE mydb;

            CREATE TABLE mydb.filtered_table1 (a UInt8, b UInt8) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.filtered_table1 values (0, 0), (0, 1), (1, 0), (1, 1);

            CREATE TABLE mydb.table (a UInt8, b UInt8) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.table values (0, 0), (0, 1), (1, 0), (1, 1);

            CREATE TABLE mydb.filtered_table2 (a UInt8, b UInt8, c UInt8, d UInt8) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.filtered_table2 values (0, 0, 0, 0), (1, 2, 3, 4), (4, 3, 2, 1), (0, 0, 6, 0);

            CREATE TABLE mydb.filtered_table3 (a UInt8, b UInt8, bb ALIAS b + 1, c UInt16 ALIAS a + bb - 1) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.filtered_table3 values (0, 0), (0, 1), (1, 0), (1, 1);

            CREATE TABLE mydb.`.filtered_table4` (a UInt8, b UInt8, c UInt16 ALIAS a + b) ENGINE MergeTree ORDER BY a;
            INSERT INTO mydb.`.filtered_table4` values (0, 0), (0, 1), (1, 0), (1, 1);

            CREATE TABLE mydb.local (a UInt8, b UInt8) ENGINE MergeTree ORDER BY a;
        """
        )

        node.query("INSERT INTO mydb.local values (2, 0), (2, 1), (1, 0), (1, 1)")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def reset_policies():
    try:
        yield
    finally:
        copy_policy_xml("normal_filters.xml")
        node.query("DROP POLICY IF EXISTS pA, pB ON mydb.filtered_table1")


def test_introspection():
    policies = [
        [
            "another ON mydb.filtered_table1",
            "another",
            "mydb",
            "filtered_table1",
            "6068883a-0e9d-f802-7e22-0144f8e66d3c",
            "users_xml",
            "1",
            0,
            0,
            "['another']",
            "[]",
        ],
        [
            "another ON mydb.filtered_table2",
            "another",
            "mydb",
            "filtered_table2",
            "c019e957-c60b-d54e-cc52-7c90dac5fb01",
            "users_xml",
            "1",
            0,
            0,
            "['another']",
            "[]",
        ],
        [
            "another ON mydb.filtered_table3",
            "another",
            "mydb",
            "filtered_table3",
            "4cb080d0-44e8-dbef-6026-346655143628",
            "users_xml",
            "1",
            0,
            0,
            "['another']",
            "[]",
        ],
        [
            "another ON mydb.local",
            "another",
            "mydb",
            "local",
            "5b23c389-7e18-06bf-a6bc-dd1afbbc0a97",
            "users_xml",
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
            "users_xml",
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
            "users_xml",
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
            "users_xml",
            "c = 1",
            0,
            0,
            "['default']",
            "[]",
        ],
        [
            "default ON mydb.local",
            "default",
            "mydb",
            "local",
            "cdacaeb5-1d97-f99d-2bb0-4574f290629c",
            "users_xml",
            "1",
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
            "another ON mydb.filtered_table1",
            "another ON mydb.filtered_table2",
            "another ON mydb.filtered_table3",
            "another ON mydb.local",
            "default ON mydb.filtered_table1",
            "default ON mydb.filtered_table2",
            "default ON mydb.filtered_table3",
            "default ON mydb.local",
        ]
    )

    assert node.query("SHOW POLICIES ON mydb.filtered_table1") == TSV(
        ["another", "default"]
    )
    assert node.query("SHOW POLICIES ON mydb.local") == TSV(["another", "default"])
    assert node.query("SHOW POLICIES ON mydb.*") == TSV(
        [
            "another ON mydb.filtered_table1",
            "another ON mydb.filtered_table2",
            "another ON mydb.filtered_table3",
            "another ON mydb.local",
            "default ON mydb.filtered_table1",
            "default ON mydb.filtered_table2",
            "default ON mydb.filtered_table3",
            "default ON mydb.local",
        ]
    )
    assert node.query("SHOW POLICIES default") == TSV(
        [
            "default ON mydb.filtered_table1",
            "default ON mydb.filtered_table2",
            "default ON mydb.filtered_table3",
            "default ON mydb.local",
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
    assert (
        node.query("SHOW CREATE POLICY default ON mydb.local")
        == "CREATE ROW POLICY default ON mydb.local FOR SELECT USING 1 TO default\n"
    )

    assert node.query("SHOW CREATE POLICY default") == TSV(
        [
            "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING a = 1 TO default",
            "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING ((a + b) < 1) OR ((c - d) > 5) TO default",
            "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING c = 1 TO default",
            "CREATE ROW POLICY default ON mydb.local FOR SELECT USING 1 TO default",
        ]
    )
    assert node.query("SHOW CREATE POLICIES ON mydb.filtered_table1") == TSV(
        [
            "CREATE ROW POLICY another ON mydb.filtered_table1 FOR SELECT USING 1 TO another",
            "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING a = 1 TO default",
        ]
    )
    assert node.query("SHOW CREATE POLICIES ON mydb.*") == TSV(
        [
            "CREATE ROW POLICY another ON mydb.filtered_table1 FOR SELECT USING 1 TO another",
            "CREATE ROW POLICY another ON mydb.filtered_table2 FOR SELECT USING 1 TO another",
            "CREATE ROW POLICY another ON mydb.filtered_table3 FOR SELECT USING 1 TO another",
            "CREATE ROW POLICY another ON mydb.local FOR SELECT USING a = 1 TO another",
            "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING a = 1 TO default",
            "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING ((a + b) < 1) OR ((c - d) > 5) TO default",
            "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING c = 1 TO default",
            "CREATE ROW POLICY default ON mydb.local FOR SELECT USING 1 TO default",
        ]
    )
    assert node.query("SHOW CREATE POLICIES") == TSV(
        [
            "CREATE ROW POLICY another ON mydb.filtered_table1 FOR SELECT USING 1 TO another",
            "CREATE ROW POLICY another ON mydb.filtered_table2 FOR SELECT USING 1 TO another",
            "CREATE ROW POLICY another ON mydb.filtered_table3 FOR SELECT USING 1 TO another",
            "CREATE ROW POLICY another ON mydb.local FOR SELECT USING a = 1 TO another",
            "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING a = 1 TO default",
            "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING ((a + b) < 1) OR ((c - d) > 5) TO default",
            "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING c = 1 TO default",
            "CREATE ROW POLICY default ON mydb.local FOR SELECT USING 1 TO default",
        ]
    )

    expected_access = (
        "CREATE ROW POLICY another ON mydb.filtered_table1 FOR SELECT USING 1 TO another\n"
        "CREATE ROW POLICY another ON mydb.filtered_table2 FOR SELECT USING 1 TO another\n"
        "CREATE ROW POLICY another ON mydb.filtered_table3 FOR SELECT USING 1 TO another\n"
        "CREATE ROW POLICY another ON mydb.local FOR SELECT USING a = 1 TO another\n"
        "CREATE ROW POLICY default ON mydb.filtered_table1 FOR SELECT USING a = 1 TO default\n"
        "CREATE ROW POLICY default ON mydb.filtered_table2 FOR SELECT USING ((a + b) < 1) OR ((c - d) > 5) TO default\n"
        "CREATE ROW POLICY default ON mydb.filtered_table3 FOR SELECT USING c = 1 TO default\n"
        "CREATE ROW POLICY default ON mydb.local FOR SELECT USING 1 TO default\n"
    )
    assert expected_access in node.query("SHOW ACCESS")

    copy_policy_xml("all_rows.xml")
    assert node.query("SHOW POLICIES") == TSV(
        [
            "another ON mydb.filtered_table1",
            "another ON mydb.filtered_table2",
            "another ON mydb.filtered_table3",
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
            "another ON mydb.filtered_table1",
            "another ON mydb.filtered_table2",
            "another ON mydb.filtered_table3",
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
    assert (
        node.query("SHOW CREATE POLICY pB ON mydb.filtered_table1")
        == "CREATE ROW POLICY pB ON mydb.filtered_table1 FOR SELECT USING a > b TO default\n"
    )

    node.query("DROP POLICY pB ON mydb.filtered_table1")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV(
        [[0, 0], [0, 1], [1, 0], [1, 1]]
    )
    assert node.query("SHOW POLICIES") == ""


def test_dcl_users_with_policies_from_users_xml():
    node.query("CREATE USER X")
    node.query("GRANT SELECT ON mydb.filtered_table1 TO X")
    assert node.query("SELECT * FROM mydb.filtered_table1") == TSV([[1, 0], [1, 1]])
    assert node.query("SELECT * FROM mydb.filtered_table1", user="X") == ""
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
    assert node.query("SELECT * FROM mydb.filtered_table1", user="Y") == ""

    # restrictive a >=b for X, none for Y
    node.query("ALTER POLICY pA ON mydb.filtered_table1 AS restrictive")
    assert node.query("SELECT * FROM mydb.filtered_table1", user="X") == ""
    assert node.query("SELECT * FROM mydb.filtered_table1", user="Y") == ""

    # permissive a >= b for X, restrictive a <= b for X, none for Y
    node.query("ALTER POLICY pA ON mydb.filtered_table1 AS permissive")
    node.query(
        "CREATE POLICY pB ON mydb.filtered_table1 FOR SELECT USING a <= b AS restrictive TO X"
    )
    assert node.query("SELECT * FROM mydb.filtered_table1", user="X") == TSV(
        [[0, 0], [1, 1]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table1", user="Y") == ""

    # permissive a >= b for X, restrictive a <= b for Y
    node.query("ALTER POLICY pB ON mydb.filtered_table1 TO Y")
    assert node.query("SELECT * FROM mydb.filtered_table1", user="X") == TSV(
        [[0, 0], [1, 0], [1, 1]]
    )
    assert node.query("SELECT * FROM mydb.filtered_table1", user="Y") == ""

    node.query("DROP POLICY pA, pB ON mydb.filtered_table1")
    node.query("DROP USER X, Y")
