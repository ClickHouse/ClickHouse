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
    macros={"shard": 1, "replica": "instance1"},
    stay_alive=True,
    with_ldap=True,
    with_zookeeper=True,
)

instance2 = cluster.add_instance(
    "instance2",
    main_configs=["configs/remote_servers.xml"],
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
    instance1.query("DROP ROLE IF EXISTS role_1")
    instance1.query("DROP ROLE IF EXISTS role_2")
    instance1.query("DROP ROLE IF EXISTS role_3")
    instance1.query("CREATE ROLE role_1")
    instance1.query("CREATE ROLE role_2")
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

    instance1.query("CREATE ROLE role_3")
    add_ldap_group(ldap_cluster, group_cn="clickhouse-role_3", member_cn="johndoe")
    # Check that non-existing role in ClickHouse is ignored during role update
    # See https://github.com/ClickHouse/ClickHouse/issues/54318
    add_ldap_group(ldap_cluster, group_cn="clickhouse-role_4", member_cn="johndoe")

    assert instance1.query(
        "select role_name from system.current_roles ORDER BY role_name",
        user="johndoe",
        password="qwertz",
    ) == TSV([["role_1"], ["role_2"], ["role_3"]])

    instance1.query("DROP ROLE role_1")
    instance1.query("DROP ROLE role_2")
    instance1.query("DROP ROLE role_3")

    delete_ldap_group(ldap_cluster, group_cn="clickhouse-role_1")
    delete_ldap_group(ldap_cluster, group_cn="clickhouse-role_2")
    delete_ldap_group(ldap_cluster, group_cn="clickhouse-role_3")
    delete_ldap_group(ldap_cluster, group_cn="clickhouse-role_4")


def test_push_role_to_other_nodes(ldap_cluster):
    instance1.query("DROP TABLE IF EXISTS distributed_table SYNC")
    instance1.query("DROP TABLE IF EXISTS local_table SYNC")
    instance2.query("DROP TABLE IF EXISTS local_table SYNC")
    instance1.query("DROP ROLE IF EXISTS role_read")

    instance1.query("CREATE ROLE role_read")
    instance1.query("GRANT SELECT ON *.* TO role_read")

    add_ldap_group(ldap_cluster, group_cn="clickhouse-role_read", member_cn="johndoe")

    assert instance1.query(
        "select currentUser()", user="johndoe", password="qwertz"
    ) == TSV([["johndoe"]])

    instance1.query(
        "CREATE TABLE IF NOT EXISTS local_table (id UInt32) ENGINE = MergeTree() ORDER BY id"
    )
    instance2.query(
        "CREATE TABLE IF NOT EXISTS local_table (id UInt32) ENGINE = MergeTree() ORDER BY id"
    )
    instance2.query("INSERT INTO local_table VALUES (1), (2), (3)")
    instance1.query(
        "CREATE TABLE IF NOT EXISTS distributed_table AS local_table ENGINE = Distributed(test_ldap_cluster, default, local_table)"
    )

    result = instance1.query(
        "SELECT sum(id) FROM distributed_table", user="johndoe", password="qwertz"
    )
    assert result.strip() == "6"

    instance1.query("DROP TABLE IF EXISTS distributed_table SYNC")
    instance1.query("DROP TABLE IF EXISTS local_table SYNC")
    instance2.query("DROP TABLE IF EXISTS local_table SYNC")
    instance2.query("DROP ROLE IF EXISTS role_read")

    delete_ldap_group(ldap_cluster, group_cn="clickhouse-role_read")


def test_remote_query_user_does_not_exist_locally(ldap_cluster):
    """
    Check that even if user does not exist locally, using it to execute remote queries is still possible
    """
    instance2.query("DROP USER IF EXISTS non_local")
    instance2.query("DROP TABLE IF EXISTS test_table sync")

    instance2.query("CREATE USER non_local")
    instance2.query("CREATE TABLE test_table (id Int16) ENGINE=Memory")
    instance2.query("INSERT INTO test_table VALUES (123)")
    instance2.query("GRANT SELECT ON default.test_table TO non_local")

    # serialize_query_plan is disabled because analysis requiers that local table exists.
    result = instance1.query(
        "SELECT * FROM remote('instance2', 'default.test_table', 'non_local') settings serialize_query_plan = 0"
    )
    assert result.strip() == "123"

    instance2.query("DROP USER IF EXISTS non_local")
    instance2.query("DROP TABLE IF EXISTS test_table SYNC")
