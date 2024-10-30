import logging
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

LDAP_ADMIN_BIND_DN = "cn=admin,dc=example,dc=org"
LDAP_ADMIN_PASSWORD = "clickhouse"

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance", main_configs=["configs/ldap_with_role_mapping.xml"], with_ldap=True
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
    assert instance.query(
        "SELECT currentUser()", user="janedoe", password="qwerty"
    ) == TSV([["janedoe"]])


def test_authentication_fail():
    # User doesn't exist.
    assert "doesnotexist: Authentication failed" in instance.query_and_get_error(
        "SELECT currentUser()", user="doesnotexist"
    )

    # Wrong password.
    assert "janedoe: Authentication failed" in instance.query_and_get_error(
        "SELECT currentUser()", user="janedoe", password="123"
    )


def test_role_mapping(ldap_cluster):
    instance.query("DROP ROLE IF EXISTS role_1")
    instance.query("DROP ROLE IF EXISTS role_2")
    instance.query("DROP ROLE IF EXISTS role_3")
    instance.query("CREATE ROLE role_1")
    instance.query("CREATE ROLE role_2")
    add_ldap_group(ldap_cluster, group_cn="clickhouse-role_1", member_cn="johndoe")
    add_ldap_group(ldap_cluster, group_cn="clickhouse-role_2", member_cn="johndoe")

    assert instance.query(
        "select currentUser()", user="johndoe", password="qwertz"
    ) == TSV([["johndoe"]])

    assert instance.query(
        "select role_name from system.current_roles ORDER BY role_name",
        user="johndoe",
        password="qwertz",
    ) == TSV([["role_1"], ["role_2"]])

    instance.query("CREATE ROLE role_3")
    add_ldap_group(ldap_cluster, group_cn="clickhouse-role_3", member_cn="johndoe")
    # Check that non-existing role in ClickHouse is ignored during role update
    # See https://github.com/ClickHouse/ClickHouse/issues/54318
    add_ldap_group(ldap_cluster, group_cn="clickhouse-role_4", member_cn="johndoe")

    assert instance.query(
        "select role_name from system.current_roles ORDER BY role_name",
        user="johndoe",
        password="qwertz",
    ) == TSV([["role_1"], ["role_2"], ["role_3"]])

    instance.query("DROP ROLE role_1")
    instance.query("DROP ROLE role_2")
    instance.query("DROP ROLE role_3")

    delete_ldap_group(ldap_cluster, group_cn="clickhouse-role_1")
    delete_ldap_group(ldap_cluster, group_cn="clickhouse-role_2")
    delete_ldap_group(ldap_cluster, group_cn="clickhouse-role_3")
    delete_ldap_group(ldap_cluster, group_cn="clickhouse-role_4")
