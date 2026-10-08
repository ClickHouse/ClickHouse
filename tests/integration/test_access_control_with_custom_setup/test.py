import logging
import random
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance")


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
        instance.query("DROP USER IF EXISTS A, B, ure, extra_user")
        instance.query("DROP ROLE IF EXISTS R1, R2, R3, R4, rre, extra_role")
        instance.query("DROP TABLE IF EXISTS tbl")
        instance.query("DROP TABLE IF EXISTS table1")
        instance.query("DROP TABLE IF EXISTS table2")


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


def test_login_as_dropped_user_xml():
    def remove_user_c_xml():
        instance.exec_in_container(
            ["bash", "-c", "rm -f /etc/clickhouse-server/users.d/user_c.xml"]
        )
        instance.query("SYSTEM RELOAD CONFIG")

    for _ in range(0, 2):
        try:
            instance.exec_in_container(
                [
                    "bash",
                    "-c",
                    """
                cat > /etc/clickhouse-server/users.d/user_c.xml << EOF
<clickhouse>
    <users>
        <C>
            <no_password/>
        </C>
    </users>
</clickhouse>
EOF""",
                ]
            )

            assert_eq_with_retry(
                instance, "SELECT name FROM system.users WHERE name='C'", "C"
            )

            remove_user_c_xml()

            expected_errors = [
                "no user with such name",
                "not found in `user directories`",
                "User has been dropped",
            ]
            while True:
                out, err = instance.query_and_get_answer_with_error("SELECT 1", user="C")
                found_error = [
                    expected_error
                    for expected_error in expected_errors
                    if (expected_error in err)
                ]
                if found_error:
                    logging.debug(f"Got error '{found_error}' just as expected")
                    break
                if out == "1\n":
                    logging.debug("Got output '1', retrying...")
                    time.sleep(0.5)
                    continue
                raise Exception(
                    f"Expected either output '1' or one of errors '{expected_errors}', got output={out} and error={err}"
                )

            assert instance.query("SELECT name FROM system.users WHERE name='C'") == ""
        finally:
            remove_user_c_xml()


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
