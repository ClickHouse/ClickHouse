import logging

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

LDAP_ADMIN_BIND_DN = "cn=admin,dc=example,dc=org"
LDAP_ADMIN_PASSWORD = "clickhouse"

cluster = ClickHouseCluster(__file__)

instance1 = cluster.add_instance(
    "instance1",
    main_configs=["configs/ldap_with_role_mapping.xml", "configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
    macros={"shard": 1, "replica": "instance1"},
    stay_alive=True,
    with_ldap=True,
    with_zookeeper=True,
)

instance2 = cluster.add_instance(
    "instance2",
    main_configs=["configs/ldap_no_role_mapping.xml", "configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
    macros={"shard": 1, "replica": "instance2"},
    stay_alive=True,
    with_zookeeper=True,
)


@pytest.fixture(scope="module", autouse=True)
def ldap_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def add_ldap_group(ldap_cluster, group_cn, member_cn):
    code, (stdout, stderr) = ldap_cluster.ldap_container.exec_run(
        [
            "sh",
            "-c",
            """echo "dn: cn={group_cn},dc=example,dc=org
objectClass: top
objectClass: groupOfNames
member: cn={member_cn},ou=users,dc=example,dc=org" | \
ldapadd -H ldap://{host}:{port} -D "{admin_bind_dn}" -x -w {admin_password}
    """.format(
                host=ldap_cluster.ldap_host,
                port=ldap_cluster.ldap_port,
                admin_bind_dn=LDAP_ADMIN_BIND_DN,
                admin_password=LDAP_ADMIN_PASSWORD,
                group_cn=group_cn,
                member_cn=member_cn,
            ),
        ],
        demux=True,
    )
    logging.debug(
        f"test_ldap_external_user_directory code:{code} stdout:{stdout}, stderr:{stderr}"
    )
    assert code == 0


def delete_ldap_group(ldap_cluster, group_cn):
    code, (stdout, stderr) = ldap_cluster.ldap_container.exec_run(
        [
            "sh",
            "-c",
            """ldapdelete -r 'cn={group_cn},dc=example,dc=org' \
-H ldap://{host}:{port} -D "{admin_bind_dn}" -x -w {admin_password}
            """.format(
                host=ldap_cluster.ldap_host,
                port=ldap_cluster.ldap_port,
                admin_bind_dn=LDAP_ADMIN_BIND_DN,
                admin_password=LDAP_ADMIN_PASSWORD,
                group_cn=group_cn,
            ),
        ],
        demux=True,
    )
    logging.debug(
        f"test_ldap_external_user_directory code:{code} stdout:{stdout}, stderr:{stderr}"
    )
    assert code == 0


# NOTE: In this test suite we have default user explicitly disabled because of `test_push_role_to_other_nodes`.
# We do it to be sure that it is not used in interserver query (this user has very permissive privileges)
# and that external roles are really passed and applied.

def test_authentication_pass():
    assert instance1.query(
        "SELECT currentUser()", user="janedoe", password="qwerty"
    ) == TSV([["janedoe"]])


def test_authentication_fail():
    # User doesn't exist.
    assert "doesnotexist: Authentication failed" in instance1.query_and_get_error(
        "SELECT currentUser()", user="doesnotexist"
    )

    # Wrong password.
    assert "janedoe: Authentication failed" in instance1.query_and_get_error(
        "SELECT currentUser()", user="janedoe", password="123"
    )


def test_role_mapping(ldap_cluster):
    instance1.query("DROP ROLE IF EXISTS role_1", user="common_user", password="qwerty")
    instance1.query("DROP ROLE IF EXISTS role_2", user="common_user", password="qwerty")
    instance1.query("DROP ROLE IF EXISTS role_3", user="common_user", password="qwerty")
    instance1.query("CREATE ROLE role_1", user="common_user", password="qwerty")
    instance1.query("CREATE ROLE role_2", user="common_user", password="qwerty")
    add_ldap_group(ldap_cluster, group_cn="clickhouse-role_1", member_cn="johndoe")
    add_ldap_group(ldap_cluster, group_cn="clickhouse-role_2", member_cn="johndoe")

    assert instance1.query(
        "select currentUser()", user="johndoe", password="qwertz"
    ) == TSV([["johndoe"]])

    assert instance1.query(
        "select role_name from system.current_roles ORDER BY role_name",
        user="johndoe",
        password="qwertz",
    ) == TSV([["role_1"], ["role_2"]])

    instance1.query("CREATE ROLE role_3", user="common_user", password="qwerty")
    add_ldap_group(ldap_cluster, group_cn="clickhouse-role_3", member_cn="johndoe")
    # Check that non-existing role in ClickHouse is ignored during role update
    # See https://github.com/ClickHouse/ClickHouse/issues/54318
    add_ldap_group(ldap_cluster, group_cn="clickhouse-role_4", member_cn="johndoe")

    assert instance1.query(
        "select role_name from system.current_roles ORDER BY role_name",
        user="johndoe",
        password="qwertz",
    ) == TSV([["role_1"], ["role_2"], ["role_3"]])

    instance1.query("DROP ROLE role_1", user="common_user", password="qwerty")
    instance1.query("DROP ROLE role_2", user="common_user", password="qwerty")
    instance1.query("DROP ROLE role_3", user="common_user", password="qwerty")

    delete_ldap_group(ldap_cluster, group_cn="clickhouse-role_1")
    delete_ldap_group(ldap_cluster, group_cn="clickhouse-role_2")
    delete_ldap_group(ldap_cluster, group_cn="clickhouse-role_3")
    delete_ldap_group(ldap_cluster, group_cn="clickhouse-role_4")


def test_push_role_to_other_nodes(ldap_cluster):
    add_ldap_group(ldap_cluster, group_cn="clickhouse-role_read", member_cn="johndoe")

    instance2.query("DROP USER IF EXISTS remote_user", user="common_user", password="qwerty")
    instance2.query("CREATE USER remote_user IDENTIFIED WITH plaintext_password BY 'qwerty'", user="common_user", password="qwerty")

    instance1.query("DROP TABLE IF EXISTS distributed_table SYNC", user="common_user", password="qwerty")
    instance1.query("DROP TABLE IF EXISTS local_table SYNC", user="common_user", password="qwerty")
    instance2.query("DROP TABLE IF EXISTS local_table SYNC", user="common_user", password="qwerty")

    instance1.query("DROP ROLE IF EXISTS role_read", user="common_user", password="qwerty")
    instance2.query("DROP ROLE IF EXISTS role_read", user="common_user", password="qwerty")

    # On both instances create a role and grant the SELECT privilege.
    instance1.query("CREATE ROLE role_read", user="common_user", password="qwerty")
    instance1.query("GRANT SELECT ON *.* TO role_read", user="common_user", password="qwerty")
    instance2.query("CREATE ROLE role_read", user="common_user", password="qwerty")
    instance2.query("GRANT SELECT ON *.* TO role_read", user="common_user", password="qwerty")

    # Verify that instance1 resolves johndoe correctly.
    assert instance1.query(
        "SELECT currentUser()", user="johndoe", password="qwertz"
    ) == TSV([["johndoe"]])

    # Create the underlying table on both nodes.
    instance1.query(
        "CREATE TABLE IF NOT EXISTS local_table (id UInt32) ENGINE = MergeTree() ORDER BY id", user="common_user", password="qwerty"
    )
    instance2.query(
        "CREATE TABLE IF NOT EXISTS local_table (id UInt32) ENGINE = MergeTree() ORDER BY id", user="common_user", password="qwerty"
    )

    # Insert some test data, only on remote node.
    instance2.query("INSERT INTO local_table VALUES (1), (2), (3)", user="common_user", password="qwerty")

    # Create a Distributed table on instance1 that points to local_table.
    instance1.query(
        "CREATE TABLE IF NOT EXISTS distributed_table AS local_table ENGINE = Distributed(test_ldap_cluster, default, local_table)", user="common_user", password="qwerty"
    )

    # Now, run the distributed query as johndoe.
    # The coordinator (instance1) will resolve johndoe's LDAP mapping,
    # and push the external role (role_read) to instance2.
    # Even though instance2 does not have role mapping, it shall honor the pushed role.
    result = instance1.query(
        "SELECT sum(id) FROM distributed_table", user="johndoe", password="qwertz"
    )
    assert result.strip() == "6"

    instance1.query("DROP TABLE IF EXISTS distributed_table SYNC", user="common_user", password="qwerty")
    instance1.query("DROP TABLE IF EXISTS local_table SYNC", user="common_user", password="qwerty")
    instance2.query("DROP TABLE IF EXISTS local_table SYNC", user="common_user", password="qwerty")
    instance1.query("DROP ROLE IF EXISTS role_read", user="common_user", password="qwerty")
    delete_ldap_group(ldap_cluster, group_cn="clickhouse-role_read")


def test_remote_query_user_does_not_exist_locally(ldap_cluster):
    """
    Check that even if user does not exist locally, using it to execute remote queries is still possible
    """
    instance2.query("DROP USER IF EXISTS non_local", user="common_user", password="qwerty")
    instance2.query("DROP TABLE IF EXISTS test_table sync", user="common_user", password="qwerty")

    instance2.query("CREATE USER non_local", user="common_user", password="qwerty")
    instance2.query("CREATE TABLE test_table (id Int16) ENGINE=Memory", user="common_user", password="qwerty")
    instance2.query("INSERT INTO test_table VALUES (123)", user="common_user", password="qwerty")
    instance2.query("GRANT SELECT ON default.test_table TO non_local", user="common_user", password="qwerty")

    # serialize_query_plan is disabled because analysis requiers that local table exists.
    result = instance1.query(
        "SELECT * FROM remote('instance2', 'default.test_table', 'non_local') settings serialize_query_plan = 0",
        user="common_user", password="qwerty"
    )
    assert result.strip() == "123"

    instance2.query("DROP USER IF EXISTS non_local", user="common_user", password="qwerty")
    instance2.query("DROP TABLE IF EXISTS test_table SYNC", user="common_user", password="qwerty")
