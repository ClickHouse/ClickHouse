import random
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance")


session_id_counter = 0


def new_session_id():
    global session_id_counter
    session_id_counter += 1
    return "session #" + str(session_id_counter)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        instance.query(
            "CREATE TABLE test_table(x UInt32, y UInt32) ENGINE = MergeTree ORDER BY tuple()"
        )
        instance.query("INSERT INTO test_table VALUES (1,5), (2,10)")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        instance.query("DROP USER IF EXISTS A, B")
        instance.query("DROP ROLE IF EXISTS R1, R2, R3, R4")


def test_create_role():
    instance.query("CREATE USER A")
    instance.query("CREATE ROLE R1")

    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT * FROM test_table", user="A"
    )

    instance.query("GRANT SELECT ON test_table TO R1")
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT * FROM test_table", user="A"
    )

    instance.query("GRANT R1 TO A")
    assert instance.query("SELECT * FROM test_table", user="A") == "1\t5\n2\t10\n"

    instance.query("REVOKE R1 FROM A")
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT * FROM test_table", user="A"
    )


def test_grant_role_to_role():
    instance.query("CREATE USER A")
    instance.query("CREATE ROLE R1")
    instance.query("CREATE ROLE R2")

    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT * FROM test_table", user="A"
    )

    instance.query("GRANT R1 TO A")
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT * FROM test_table", user="A"
    )

    instance.query("GRANT R2 TO R1")
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT * FROM test_table", user="A"
    )

    instance.query("GRANT SELECT ON test_table TO R2")
    assert instance.query("SELECT * FROM test_table", user="A") == "1\t5\n2\t10\n"


def test_combine_privileges():
    instance.query("CREATE USER A ")
    instance.query("CREATE ROLE R1")
    instance.query("CREATE ROLE R2")

    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT * FROM test_table", user="A"
    )

    instance.query("GRANT R1 TO A")
    instance.query("GRANT SELECT(x) ON test_table TO R1")
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT * FROM test_table", user="A"
    )
    assert instance.query("SELECT x FROM test_table", user="A") == "1\n2\n"

    instance.query("GRANT SELECT(y) ON test_table TO R2")
    instance.query("GRANT R2 TO A")
    assert instance.query("SELECT * FROM test_table", user="A") == "1\t5\n2\t10\n"


def test_admin_option():
    instance.query("CREATE USER A")
    instance.query("CREATE USER B")
    instance.query("CREATE ROLE R1")

    instance.query("GRANT SELECT ON test_table TO R1")
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT * FROM test_table", user="B"
    )

    instance.query("GRANT R1 TO A")
    assert "Not enough privileges" in instance.query_and_get_error(
        "GRANT R1 TO B", user="A"
    )
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT * FROM test_table", user="B"
    )

    instance.query("GRANT R1 TO A WITH ADMIN OPTION")
    instance.query("GRANT R1 TO B", user="A")
    assert instance.query("SELECT * FROM test_table", user="B") == "1\t5\n2\t10\n"


def test_revoke_requires_admin_option():
    instance.query("CREATE USER A, B")
    instance.query("CREATE ROLE R1, R2")

    instance.query("GRANT R1 TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT R1 TO B\n"

    expected_error = "necessary to have the role R1 granted"
    assert expected_error in instance.query_and_get_error("REVOKE R1 FROM B", user="A")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT R1 TO B\n"

    instance.query("GRANT R1 TO A")
    expected_error = "granted, but without ADMIN option"
    assert expected_error in instance.query_and_get_error("REVOKE R1 FROM B", user="A")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT R1 TO B\n"

    instance.query("GRANT R1 TO A WITH ADMIN OPTION")
    instance.query("REVOKE R1 FROM B", user="A")
    assert instance.query("SHOW GRANTS FOR B") == ""

    instance.query("GRANT R1 TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT R1 TO B\n"
    instance.query("REVOKE ALL FROM B", user="A")
    assert instance.query("SHOW GRANTS FOR B") == ""

    instance.query("GRANT R1, R2 TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT R1, R2 TO B\n"
    expected_error = "necessary to have the role R2 granted"
    assert expected_error in instance.query_and_get_error("REVOKE ALL FROM B", user="A")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT R1, R2 TO B\n"
    instance.query("REVOKE ALL EXCEPT R2 FROM B", user="A")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT R2 TO B\n"
    instance.query("GRANT R2 TO A WITH ADMIN OPTION")
    instance.query("REVOKE ALL FROM B", user="A")
    assert instance.query("SHOW GRANTS FOR B") == ""

    instance.query("GRANT R1, R2 TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT R1, R2 TO B\n"
    instance.query("REVOKE ALL FROM B", user="A")
    assert instance.query("SHOW GRANTS FOR B") == ""


def test_set_role():
    instance.query("CREATE USER A")
    instance.query("CREATE ROLE R1, R2")
    instance.query("GRANT R1, R2 TO A")

    session_id = new_session_id()
    assert instance.http_query(
        "SHOW CURRENT ROLES", user="A", params={"session_id": session_id}
    ) == TSV([["R1", 0, 1], ["R2", 0, 1]])

    instance.http_query("SET ROLE R1", user="A", params={"session_id": session_id})
    assert instance.http_query(
        "SHOW CURRENT ROLES", user="A", params={"session_id": session_id}
    ) == TSV([["R1", 0, 1]])

    instance.http_query("SET ROLE R2", user="A", params={"session_id": session_id})
    assert instance.http_query(
        "SHOW CURRENT ROLES", user="A", params={"session_id": session_id}
    ) == TSV([["R2", 0, 1]])

    instance.http_query("SET ROLE NONE", user="A", params={"session_id": session_id})
    assert instance.http_query(
        "SHOW CURRENT ROLES", user="A", params={"session_id": session_id}
    ) == TSV([])

    instance.http_query("SET ROLE DEFAULT", user="A", params={"session_id": session_id})
    assert instance.http_query(
        "SHOW CURRENT ROLES", user="A", params={"session_id": session_id}
    ) == TSV([["R1", 0, 1], ["R2", 0, 1]])


def test_changing_default_roles_affects_new_sessions_only():
    instance.query("CREATE USER A")
    instance.query("CREATE ROLE R1, R2")
    instance.query("GRANT R1, R2 TO A")

    session_id = new_session_id()
    assert instance.http_query(
        "SHOW CURRENT ROLES", user="A", params={"session_id": session_id}
    ) == TSV([["R1", 0, 1], ["R2", 0, 1]])
    instance.query("SET DEFAULT ROLE R2 TO A")
    assert instance.http_query(
        "SHOW CURRENT ROLES", user="A", params={"session_id": session_id}
    ) == TSV([["R1", 0, 0], ["R2", 0, 1]])

    other_session_id = new_session_id()
    assert instance.http_query(
        "SHOW CURRENT ROLES", user="A", params={"session_id": other_session_id}
    ) == TSV([["R2", 0, 1]])


def test_introspection():
    instance.query("CREATE USER A")
    instance.query("CREATE USER B")
    instance.query("CREATE ROLE R1")
    instance.query("CREATE ROLE R2")
    instance.query("GRANT R1 TO A")
    instance.query("GRANT R2 TO B WITH ADMIN OPTION")
    instance.query("GRANT SELECT ON test.table TO A, R2")
    instance.query("GRANT CREATE ON *.* TO B WITH GRANT OPTION")
    instance.query("REVOKE SELECT(x) ON test.table FROM R2")

    assert instance.query("SHOW ROLES") == TSV(["R1", "R2"])
    assert instance.query("SHOW CREATE ROLE R1") == TSV(["CREATE ROLE R1"])
    assert instance.query("SHOW CREATE ROLE R2") == TSV(["CREATE ROLE R2"])
    assert instance.query("SHOW CREATE ROLES R1, R2") == TSV(
        ["CREATE ROLE R1", "CREATE ROLE R2"]
    )
    assert instance.query("SHOW CREATE ROLES") == TSV(
        ["CREATE ROLE R1", "CREATE ROLE R2"]
    )

    assert instance.query("SHOW GRANTS FOR A") == TSV(
        ["GRANT SELECT ON test.`table` TO A", "GRANT R1 TO A"]
    )
    assert instance.query("SHOW GRANTS FOR B") == TSV(
        [
            "GRANT CREATE ON *.* TO B WITH GRANT OPTION",
            "GRANT R2 TO B WITH ADMIN OPTION",
        ]
    )
    assert instance.query("SHOW GRANTS FOR R1") == ""
    assert instance.query("SHOW GRANTS FOR R2") == TSV(
        [
            "GRANT SELECT ON test.`table` TO R2",
            "REVOKE SELECT(x) ON test.`table` FROM R2",
        ]
    )

    assert instance.query("SHOW GRANTS", user="A") == TSV(
        ["GRANT SELECT ON test.`table` TO A", "GRANT R1 TO A"]
    )

    assert instance.query("SHOW GRANTS FOR R1", user="A") == TSV([])
    with pytest.raises(QueryRuntimeException, match="Not enough privileges"):
        assert instance.query("SHOW GRANTS FOR R2", user="A")

    assert instance.query("SHOW GRANTS", user="B") == TSV(
        [
            "GRANT CREATE ON *.* TO B WITH GRANT OPTION",
            "GRANT R2 TO B WITH ADMIN OPTION",
        ]
    )
    assert instance.query("SHOW CURRENT ROLES", user="A") == TSV([["R1", 0, 1]])
    assert instance.query("SHOW CURRENT ROLES", user="B") == TSV([["R2", 1, 1]])
    assert instance.query("SHOW ENABLED ROLES", user="A") == TSV([["R1", 0, 1, 1]])
    assert instance.query("SHOW ENABLED ROLES", user="B") == TSV([["R2", 1, 1, 1]])

    expected_access1 = "CREATE ROLE R1\n" "CREATE ROLE R2\n"
    expected_access2 = "GRANT R1 TO A\n"
    expected_access3 = "GRANT R2 TO B WITH ADMIN OPTION"
    assert expected_access1 in instance.query("SHOW ACCESS")
    assert expected_access2 in instance.query("SHOW ACCESS")
    assert expected_access3 in instance.query("SHOW ACCESS")

    assert instance.query(
        "SELECT name, storage from system.roles WHERE name IN ('R1', 'R2') ORDER BY name"
    ) == TSV([["R1", "local_directory"], ["R2", "local_directory"]])

    assert instance.query(
        "SELECT * from system.grants WHERE user_name IN ('A', 'B') OR role_name IN ('R1', 'R2') ORDER BY user_name, role_name, access_type, database, table, column, is_partial_revoke, grant_option"
    ) == TSV(
        [
            ["A", "\\N", "SELECT", "test", "table", "\\N", 0, 0],
            ["B", "\\N", "CREATE", "\\N", "\\N", "\\N", 0, 1],
            ["\\N", "R2", "SELECT", "test", "table", "x", 1, 0],
            ["\\N", "R2", "SELECT", "test", "table", "\\N", 0, 0],
        ]
    )

    assert instance.query(
        "SELECT user_name, role_name, granted_role_name, granted_role_is_default, with_admin_option from system.role_grants WHERE user_name IN ('A', 'B') OR role_name IN ('R1', 'R2') ORDER BY user_name, role_name, granted_role_name"
    ) == TSV([["A", "\\N", "R1", 1, 0], ["B", "\\N", "R2", 1, 1]])

    assert instance.query(
        "SELECT * from system.current_roles ORDER BY role_name", user="A"
    ) == TSV([["R1", 0, 1]])
    assert instance.query(
        "SELECT * from system.current_roles ORDER BY role_name", user="B"
    ) == TSV([["R2", 1, 1]])
    assert instance.query(
        "SELECT * from system.enabled_roles ORDER BY role_name", user="A"
    ) == TSV([["R1", 0, 1, 1]])
    assert instance.query(
        "SELECT * from system.enabled_roles ORDER BY role_name", user="B"
    ) == TSV([["R2", 1, 1, 1]])


def test_function_current_roles():
    instance.query("CREATE USER A")
    instance.query("CREATE ROLE R1, R2, R3, R4")
    instance.query("GRANT R4 TO R2")
    instance.query("GRANT R1,R2,R3 TO A")

    session_id = new_session_id()
    assert (
        instance.http_query(
            "SELECT defaultRoles(), currentRoles(), enabledRoles()",
            user="A",
            params={"session_id": session_id},
        )
        == "['R1','R2','R3']\t['R1','R2','R3']\t['R1','R2','R3','R4']\n"
    )

    instance.http_query("SET ROLE R1", user="A", params={"session_id": session_id})
    assert (
        instance.http_query(
            "SELECT defaultRoles(), currentRoles(), enabledRoles()",
            user="A",
            params={"session_id": session_id},
        )
        == "['R1','R2','R3']\t['R1']\t['R1']\n"
    )

    instance.http_query("SET ROLE R2", user="A", params={"session_id": session_id})
    assert (
        instance.http_query(
            "SELECT defaultRoles(), currentRoles(), enabledRoles()",
            user="A",
            params={"session_id": session_id},
        )
        == "['R1','R2','R3']\t['R2']\t['R2','R4']\n"
    )

    instance.http_query("SET ROLE NONE", user="A", params={"session_id": session_id})
    assert (
        instance.http_query(
            "SELECT defaultRoles(), currentRoles(), enabledRoles()",
            user="A",
            params={"session_id": session_id},
        )
        == "['R1','R2','R3']\t[]\t[]\n"
    )

    instance.http_query("SET ROLE DEFAULT", user="A", params={"session_id": session_id})
    assert (
        instance.http_query(
            "SELECT defaultRoles(), currentRoles(), enabledRoles()",
            user="A",
            params={"session_id": session_id},
        )
        == "['R1','R2','R3']\t['R1','R2','R3']\t['R1','R2','R3','R4']\n"
    )

    instance.query("SET DEFAULT ROLE R2 TO A")
    assert (
        instance.http_query(
            "SELECT defaultRoles(), currentRoles(), enabledRoles()",
            user="A",
            params={"session_id": session_id},
        )
        == "['R2']\t['R1','R2','R3']\t['R1','R2','R3','R4']\n"
    )

    instance.query("REVOKE R3 FROM A")
    assert (
        instance.http_query(
            "SELECT defaultRoles(), currentRoles(), enabledRoles()",
            user="A",
            params={"session_id": session_id},
        )
        == "['R2']\t['R1','R2']\t['R1','R2','R4']\n"
    )

    instance.query("REVOKE R2 FROM A")
    assert (
        instance.http_query(
            "SELECT defaultRoles(), currentRoles(), enabledRoles()",
            user="A",
            params={"session_id": session_id},
        )
        == "[]\t['R1']\t['R1']\n"
    )

    instance.query("SET DEFAULT ROLE ALL TO A")
    assert (
        instance.http_query(
            "SELECT defaultRoles(), currentRoles(), enabledRoles()",
            user="A",
            params={"session_id": session_id},
        )
        == "['R1']\t['R1']\t['R1']\n"
    )


@pytest.mark.parametrize("with_extra_role", [False, True])
def test_role_expiration(with_extra_role):
    instance.query("CREATE ROLE rre")
    instance.query("CREATE USER ure DEFAULT ROLE rre")

    instance.query("CREATE TABLE table1 (id Int) Engine=Log")
    instance.query("CREATE TABLE table2 (id Int) Engine=Log")
    instance.query("INSERT INTO table1 VALUES (1)")
    instance.query("INSERT INTO table2 VALUES (2)")

    instance.query("GRANT SELECT ON table1 TO rre")

    assert instance.query("SELECT * FROM table1", user="ure") == "1\n"
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT * FROM table2", user="ure"
    )

    # access_control_improvements/role_cache_expiration_time_seconds value is 2 for the test
    # so we wait >2 seconds until the role is expired
    time.sleep(5)

    if with_extra_role:
        # Expiration of role "rre" from the role cache can be caused by another role being used.
        instance.query("CREATE ROLE extra_role")
        instance.query("CREATE USER extra_user DEFAULT ROLE extra_role")
        instance.query("GRANT SELECT ON table1 TO extra_role")
        assert instance.query("SELECT * FROM table1", user="extra_user") == "1\n"

    instance.query("GRANT SELECT ON table2 TO rre")
    assert instance.query("SELECT * FROM table1", user="ure") == "1\n"
    assert instance.query("SELECT * FROM table2", user="ure") == "2\n"

    instance.query("DROP ROLE rre")
    instance.query("DROP USER ure")
    instance.query("DROP TABLE table1")
    instance.query("DROP TABLE table2")

    if with_extra_role:
        instance.query("DROP ROLE extra_role")
        instance.query("DROP USER extra_user")


def test_roles_cache():
    # This test takes 20 seconds.
    test_time = 20

    # Three users A, B, C.
    users = ["A", "B", "C"]
    instance.query("CREATE USER " + ", ".join(users))

    # Table "tbl" has 10 columns. Each of the users has access to a different set of columns.
    num_columns = 10
    columns = [f"x{i}" for i in range(1, num_columns + 1)]
    columns_with_types = [column + " Int64" for column in columns]
    columns_with_types_comma_separated = ", ".join(columns_with_types)
    values = list(range(1, num_columns + 1))
    values_comma_separated = ", ".join([str(value) for value in values])
    instance.query(
        f"CREATE TABLE tbl ({columns_with_types_comma_separated}) ENGINE=MergeTree ORDER BY tuple()"
    )
    instance.query(f"INSERT INTO tbl VALUES ({values_comma_separated})")
    columns_to_values = dict([(f"x{i}", i) for i in range(1, num_columns + 1)])

    # In this test we create and modify roles multiple times along with updating the following variables.
    # Then we check that each of the users has access to the expected set of columns.
    roles = []
    users_to_roles = dict([(user, []) for user in users])
    roles_to_columns = {}

    # Checks that each of the users can access the expected set of columns and can't access other columns.
    def check():
        for user in random.sample(users, len(users)):
            expected_roles = users_to_roles[user]
            expected_columns = list(
                set(sum([roles_to_columns[role] for role in expected_roles], []))
            )
            expected_result = sorted(
                [columns_to_values[column] for column in expected_columns]
            )
            query = " UNION ALL ".join(
                [
                    f"SELECT * FROM viewIfPermitted(SELECT {column} AS c FROM tbl ELSE null('c Int64'))"
                    for column in columns
                ]
            )
            result = instance.query(query, user=user).splitlines()
            result = sorted([int(value) for value in result])
            ok = result == expected_result
            if not ok:
                print(f"Show grants for {user}:")
                print(
                    instance.query(
                        "SHOW GRANTS FOR " + ", ".join([user] + expected_roles)
                    )
                )
                print(f"Expected result: {expected_result}")
                print(f"Got unexpected result: {result}")
            assert ok

    # Grants one of our roles a permission to access one of the columns.
    def grant_column():
        columns_used_in_roles = sum(roles_to_columns.values(), [])
        columns_to_choose = [
            column for column in columns if column not in columns_used_in_roles
        ]
        if not columns_to_choose or not roles:
            return False
        column = random.choice(columns_to_choose)
        role = random.choice(roles)
        instance.query(f"GRANT SELECT({column}) ON tbl TO {role}")
        roles_to_columns[role].append(column)
        return True

    # Revokes a permission to access one of the granted column from all our roles.
    def revoke_column():
        columns_used_in_roles = sum(roles_to_columns.values(), [])
        columns_to_choose = list(set(columns_used_in_roles))
        if not columns_to_choose or not roles:
            return False
        column = random.choice(columns_to_choose)
        roles_str = ", ".join(roles)
        instance.query(f"REVOKE SELECT({column}) ON tbl FROM {roles_str}")
        for role in roles_to_columns:
            if column in roles_to_columns[role]:
                roles_to_columns[role].remove(column)
        return True

    # Creates a role and grants it to one of the users.
    def create_role():
        for role in ["R1", "R2", "R3"]:
            if role not in roles:
                instance.query(f"CREATE ROLE {role}")
                roles.append(role)
                if role not in roles_to_columns:
                    roles_to_columns[role] = []
        if "R1" not in users_to_roles["A"]:
            instance.query("GRANT R1 TO A")
            users_to_roles["A"].append("R1")
        elif "R2" not in users_to_roles["B"]:
            instance.query("GRANT R2 TO B")
            users_to_roles["B"].append("R2")
        elif "R3" not in users_to_roles["B"]:
            instance.query("GRANT R3 TO R2")
            users_to_roles["B"].append("R3")
        elif "R3" not in users_to_roles["C"]:
            instance.query("GRANT R3 TO C")
            users_to_roles["C"].append("R3")
        else:
            return False
        return True

    # Drops one of our roles.
    def drop_role():
        if not roles:
            return False
        role = random.choice(roles)
        instance.query(f"DROP ROLE {role}")
        roles.remove(role)
        for u in users_to_roles:
            if role in users_to_roles[u]:
                users_to_roles[u].remove(role)
        del roles_to_columns[role]
        if (role == "R2") and ("R3" in users_to_roles["B"]):
            users_to_roles["B"].remove("R3")
        return True

    # Modifies some grants or roles randomly.
    def modify():
        while True:
            rnd = random.random()
            if rnd < 0.4:
                if grant_column():
                    break
            elif rnd < 0.5:
                if revoke_column():
                    break
            elif rnd < 0.9:
                if create_role():
                    break
            else:
                if drop_role():
                    break

    def maybe_modify():
        if random.random() < 0.9:
            modify()
            modify()

    # Sleeping is necessary in this test because the role cache in ClickHouse has expiration timeout.
    def maybe_sleep():
        if random.random() < 0.1:
            # "role_cache_expiration_time_seconds" is set to 2 seconds in the test configuration.
            # We need a sleep longer than that in this test sometimes.
            seconds = random.random() * 5
            print(f"Sleeping {seconds} seconds")
            time.sleep(seconds)

    # Main part of the test.
    start_time = time.time()
    end_time = start_time + test_time

    while time.time() < end_time:
        check()
        maybe_sleep()
        maybe_modify()
        maybe_sleep()

    check()

    instance.query("DROP USER " + ", ".join(users))
    if roles:
        instance.query("DROP ROLE " + ", ".join(roles))
    instance.query("DROP TABLE tbl")
