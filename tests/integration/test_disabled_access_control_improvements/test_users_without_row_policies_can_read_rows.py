import os
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/users_without_row_policies_can_read_rows.xml"],
    user_configs=[
        "configs/users.d/row_policy.xml",
        "configs/users.d/another_user.xml",
    ],
)


def copy_policy_xml(local_file_name, reload_immediately=True):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    node.copy_file_to_container(
        os.path.join(script_dir, local_file_name),
        "/etc/clickhouse-server/users.d/row_policy.xml",
    )
    if reload_immediately:
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
