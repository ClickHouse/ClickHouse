import logging
import os
import time

import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check
from helpers.test_tools import TSV

LDAP_ADMIN_BIND_DN = "cn=admin,dc=example,dc=org"
LDAP_ADMIN_PASSWORD = "clickhouse"

DOCKER_COMPOSE_PATH = get_docker_compose_path()

cluster = ClickHouseCluster(__file__)

# Instance with follow_referrals=true: role mapping should work via referral chasing.
# ClickHouse connects to openldap2 for bind (users live in dc=referral,dc=org),
# then role mapping search targets dc=example,dc=org which is NOT served by openldap2,
# so openldap2 returns a default referral to openldap where the groups actually exist.
instance_follow = cluster.add_instance(
    "instance_follow",
    main_configs=["configs/ldap_follow_referrals_true.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
    with_ldap=True,
)

# Instance with follow_referrals=false: role mapping should NOT work (referral ignored)
instance_no_follow = cluster.add_instance(
    "instance_no_follow",
    main_configs=["configs/ldap_follow_referrals_false.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)


def wait_openldap2_ready(timeout=180):
    openldap2_docker_id = cluster.get_instance_docker_id("openldap2")
    start = time.time()
    attempts = 0
    logging.info("Waiting for openldap2 readiness")
    while time.time() - start < timeout:
        attempts += 1
        try:
            cluster.exec_in_container(
                openldap2_docker_id,
                [
                    "bash",
                    "-c",
                    "test -f /tmp/.openldap-initialized"
                    " && /opt/bitnami/openldap/bin/ldapsearch -x -H ldap://localhost:1389"
                    " -D cn=admin,dc=referral,dc=org -w clickhouse -b dc=referral,dc=org"
                    " | grep -c '^dn: cn=janedoe,ou=users,dc=referral,dc=org$'"
                    " | grep 1 >> /dev/null",
                ],
                user="root",
            )
            logging.info("openldap2 is ready")
            return
        except Exception as ex:
            if attempts % 10 == 0:
                logging.info("openldap2 not ready after %s attempts: %s", attempts, str(ex))
            else:
                logging.debug("openldap2 not ready yet: %s", str(ex))
            time.sleep(1)
    raise Exception("Timed out waiting for openldap2")


def add_ldap_group(group_cn, member_cn):
    """Add a group to the primary LDAP server (openldap).

    The member DN uses dc=referral,dc=org because that is the bind_dn
    that ClickHouse substitutes into the role mapping search filter
    ({bind_dn} = cn=<user>,ou=users,dc=referral,dc=org).
    """
    code, (stdout, stderr) = cluster.ldap_container.exec_run(
        [
            "sh",
            "-c",
            """echo "dn: cn={group_cn},dc=example,dc=org
objectClass: top
objectClass: groupOfNames
cn: {group_cn}
member: cn={member_cn},ou=users,dc=referral,dc=org" | \
ldapadd -H ldap://{host}:{port} -D "{admin_bind_dn}" -x -w {admin_password}
    """.format(
                host=cluster.ldap_host,
                port=cluster.ldap_port,
                admin_bind_dn=LDAP_ADMIN_BIND_DN,
                admin_password=LDAP_ADMIN_PASSWORD,
                group_cn=group_cn,
                member_cn=member_cn,
            ),
        ],
        demux=True,
    )
    logging.debug(
        f"add_ldap_group code:{code} stdout:{stdout}, stderr:{stderr}"
    )
    assert code == 0, f"ldapadd failed: code={code}, stdout={stdout!r}, stderr={stderr!r}"


def delete_ldap_group(group_cn):
    code, (stdout, stderr) = cluster.ldap_container.exec_run(
        [
            "sh",
            "-c",
            """ldapdelete -r 'cn={group_cn},dc=example,dc=org' \
-H ldap://{host}:{port} -D "{admin_bind_dn}" -x -w {admin_password}
            """.format(
                host=cluster.ldap_host,
                port=cluster.ldap_port,
                admin_bind_dn=LDAP_ADMIN_BIND_DN,
                admin_password=LDAP_ADMIN_PASSWORD,
                group_cn=group_cn,
            ),
        ],
        demux=True,
    )
    logging.debug(
        f"delete_ldap_group code:{code} stdout:{stdout}, stderr:{stderr}"
    )
    assert code == 0, f"ldapdelete failed: code={code}, stdout={stdout!r}, stderr={stderr!r}"


@pytest.fixture(scope="module", autouse=True)
def ldap_cluster():
    docker_compose_ldap2 = os.path.join(
        DOCKER_COMPOSE_PATH, "docker_compose_ldap2.yml"
    )
    try:
        cluster.start()

        # Start the second LDAP server (referral server).
        # openldap2 serves dc=referral,dc=org and has a default referral
        # pointing to openldap:1389 for any DN it cannot resolve locally.
        run_and_check(
            cluster.compose_cmd(
                "-f",
                docker_compose_ldap2,
                "up",
                "--force-recreate",
                "-d",
                "--no-build",
            )
        )
        wait_openldap2_ready()

        yield cluster
    finally:
        # Tear down openldap2 explicitly since it was started outside of
        # cluster.start() and cluster.shutdown() does not know about it.
        run_and_check(
            cluster.compose_cmd(
                "-f",
                docker_compose_ldap2,
                "down",
                "--volumes",
            ),
            nothrow=True,
        )
        cluster.shutdown()


def test_follow_referrals_true(ldap_cluster):
    """With follow_referrals=true, the role mapping search targets dc=example,dc=org
    on openldap2 which does not serve that suffix. openldap2 returns a default referral
    to openldap:1389. The LDAP library chases the referral and finds the group,
    so the user authenticates and gets the mapped role."""
    instance_follow.query(
        "DROP ROLE IF EXISTS role_ref", user="common_user", password="qwerty"
    )
    try:
        instance_follow.query(
            "CREATE ROLE role_ref", user="common_user", password="qwerty"
        )
        add_ldap_group(group_cn="clickhouse-role_ref", member_cn="johndoe")

        assert instance_follow.query(
            "SELECT currentUser()", user="johndoe", password="qwertz"
        ) == TSV([["johndoe"]])

        result = instance_follow.query(
            "SELECT role_name FROM system.current_roles ORDER BY role_name",
            user="johndoe",
            password="qwertz",
        )
        assert result == TSV([["role_ref"]])
    finally:
        instance_follow.query(
            "DROP ROLE IF EXISTS role_ref", user="common_user", password="qwerty"
        )
        try:
            delete_ldap_group(group_cn="clickhouse-role_ref")
        except AssertionError:
            logging.warning("Could not delete LDAP group clickhouse-role_ref")


def test_follow_referrals_false(ldap_cluster):
    """With follow_referrals=false, the role mapping search targets dc=example,dc=org
    on openldap2 which returns a referral. The LDAP library does not chase it,
    so the search fails and authentication is rejected."""
    instance_no_follow.query(
        "DROP ROLE IF EXISTS role_noref", user="common_user", password="qwerty"
    )
    try:
        instance_no_follow.query(
            "CREATE ROLE role_noref", user="common_user", password="qwerty"
        )
        add_ldap_group(group_cn="clickhouse-role_noref", member_cn="johndoe")

        error = instance_no_follow.query_and_get_error(
            "SELECT currentUser()", user="johndoe", password="qwertz"
        )
        err = error.lower()
        assert "johndoe" in err and "authentication failed" in err, (
            f"Unexpected error message: {error}"
        )
    finally:
        instance_no_follow.query(
            "DROP ROLE IF EXISTS role_noref", user="common_user", password="qwerty"
        )
        try:
            delete_ldap_group(group_cn="clickhouse-role_noref")
        except AssertionError:
            logging.warning("Could not delete LDAP group clickhouse-role_noref")
