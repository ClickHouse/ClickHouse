import pytest
from os import getuid
import time
from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)


cluster.add_instance(
    "instance1",
    main_configs=["configs/clickhouse/config.d/logger.xml",
                #   "configs/clickhouse/config.d/zookeeper.xml",
                  "configs/clickhouse/config.d/ldap_servers.xml",
                #   "configs/clickhouse/config.d/user_directories.xml",
                  "configs/clickhouse/config.d/remote_servers.xml"],
    user_configs=["configs/clickhouse/users.xml",],
    macros={"shard": 1, "replica": "instance1"},
    stay_alive=True,
    with_ldap=True,
    with_zookeeper=True,
)
cluster.add_instance(
    "instance2",
    main_configs=["configs/clickhouse/config.d/logger.xml",
                #   "configs/clickhouse/config.d/zookeeper.xml",
                  "configs/clickhouse/config.d/ldap_servers.xml",
                #   "configs/clickhouse/config.d/user_directories.xml",
                  "configs/clickhouse/config.d/remote_servers.xml"],
    user_configs=["configs/clickhouse/users.xml",],
    macros={"shard": 1, "replica": "instance2"},
    stay_alive=True,
    with_ldap=True,
    with_zookeeper=True,
)
cluster.add_instance(
    "instance3",
    main_configs=["configs/clickhouse/config.d/logger.xml",
                #   "configs/clickhouse/config.d/zookeeper.xml",
                  "configs/clickhouse/config.d/ldap_servers.xml",
                #   "configs/clickhouse/config.d/user_directories.xml",
                  "configs/clickhouse/config.d/remote_servers.xml"],
    user_configs=["configs/clickhouse/users.xml",],
    macros={"shard": 1, "replica": "instance3"},
    stay_alive=True,
    with_ldap=True,
    with_zookeeper=True,
)

instances = [cluster.instances["instance1"], cluster.instances["instance2"], cluster.instances["instance3"]]


ldap_server = {
    "host": "my_ldap_server",
    "port": "389",
    "enable_tls": "no",
    "bind_dn": "cn={user_name},ou=users,dc=company,dc=com",
}

# Fixtures


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


# Helpers


def create_table(node, on_cluster, name=None):
    if name is None:
        name = f"tbl_{getuid()}"
    
    node.query(
        f"CREATE TABLE {name} ON CLUSTER {on_cluster} (d Date, a String, b UInt8, x String, y Int8) "
        f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') "
        "PARTITION BY y ORDER BY (d, b)")
    
    return name


def create_distributed_table(node, on_cluster, over, name=None):
    if name is None:
        name = f"dis_tbl_{getuid()}"

    node.query(
        f"CREATE TABLE {name} ON CLUSTER {on_cluster} AS {over} "
        f"ENGINE = Distributed({on_cluster}, default, {over}, rand())")
    
    return name


def drop_table(node, name, on_cluster):
    node.query(f"DROP TABLE IF EXISTS {name} ON CLUSTER {on_cluster} SYNC")


def grant_select(cluster, privilege, role_or_user, node):
    """Grant select privilege on a table on a given cluster
    to a role or a user.
    """
    node.query(f"GRANT ON CLUSTER {cluster} {privilege} TO {role_or_user}")


def revoke_select(cluster, privilege, role_or_user, node):
    node.query(f"REVOKE ON CLUSTER {cluster} {privilege} FROM {role_or_user}")


def add_user_to_ldap_command(
    cn,
    userpassword,
    givenname=None,
    homedirectory=None,
    sn=None,
    uid=None,
    uidnumber=None,
):
    if uid is None:
        uid = cn
    if givenname is None:
        givenname = "John"
    if homedirectory is None:
        homedirectory = "/home/users"
    if sn is None:
        sn = "User"
    if uidnumber is None:
        uidnumber = 2000

    user = {
        "dn": f"cn={cn},ou=users,dc=company,dc=com",
        "cn": cn,
        "gidnumber": 501,
        "givenname": givenname,
        "homedirectory": homedirectory,
        "objectclass": ["inetOrgPerson", "posixAccount", "top"],
        "sn": sn,
        "uid": uid,
        "uidnumber": uidnumber,
        "userpassword": userpassword,
        "_server": cluster.ldap_host,
    }

    lines = []

    for key, value in list(user.items()):
        if key.startswith("_"):
            continue
        elif key == "objectclass":
            for cls in value:
                lines.append(f"objectclass: {cls}")
        else:
            lines.append(f"{key}: {value}")

    ldif = "\n".join(lines)

    return f'echo -e "{ldif}" | ldapadd -x -H ldap://localhost -D "cn=admin,dc=company,dc=com" -w admin'


def rm_user_from_ldap_command(user):
    return f"ldapdelete -x -H ldap://localhost -D \"cn=admin,dc=company,dc=com\" -w admin \"{user['dn']}\""


def add_ldap_group_command(cn, gidnumber=None, _gidnumber=[600]):
    """Add new group entry to LDAP."""
    _gidnumber[0] += 1

    if gidnumber is None:
        gidnumber = _gidnumber[0]

    group = {
        "dn": f"cn={cn},ou=groups,dc=company,dc=com",
        "objectclass": ["top", "groupOfUniqueNames"],
        "uniquemember": "cn=admin,dc=company,dc=com",
        "_server": cluster.ldap_host,
    }

    lines = []

    for key, value in list(group.items()):
        if key.startswith("_"):
            continue
        elif type(value) is list:
            for v in value:
                lines.append(f"{key}: {v}")
        else:
            lines.append(f"{key}: {value}")

    ldif = "\n".join(lines)

    return f'echo -e "{ldif}" | ldapadd -x -H ldap://localhost -D "cn=admin,dc=company,dc=com" -w admin'


def add_ldap_user_to_group_command(user, group_cn):
    """Add user to a group in LDAP."""
    ldif = (
        f"dn: {group_cn['dn']}\n"
        "changetype: modify\n"
        "add: uniquemember\n"
        f"uniquemember: {user['dn']}"
    )

    return f'echo -e "{ldif}" | ldapmodify -x -H ldap://localhost -D "cn=admin,dc=company,dc=com" -w admin'

def add_rbac_user(user, node):
    username = user.get("username", None) or user["cn"]
    password = user.get("password", None) or user["userpassword"]
    node.query(f"CREATE USER OR REPLACE {username} IDENTIFIED WITH PLAINTEXT_PASSWORD BY '{password}'")


def rm_rbac_users(user, node):
    username = user.get("username", None) or user["cn"]
    node.query(f"DROP USER IF EXISTS {username}")
    

def add_rbac_role(role, node):
    node.query(f"CREATE ROLE OR REPLACE {role}")


def rm_rbac_roles(role, node):
    node.query(f"DROP ROLE IF EXISTS {role}")









def outline_test_select_using_mapped_role(cluster, role_name, role_mapped, user):
    """Check accessing normal and distributed table using
    a user and the specified role that is either granted
    rights to access the tables or not and is or is not assigned to the user
    from all cluster nodes.
    """
    # default cluster node
    node = instances[0]

    query_settings = {"user": user["username"], "password": user["password"]}

    # create base table on cluster
    src_table = create_table(node=node, on_cluster=cluster)

    # create distristibuted table over base table on cluster
    dist_table = create_distributed_table(on_cluster=cluster, over=src_table, node=node)

    # check that grants for the user
    for instance in instances:
        for _ in range(10):
            r = instance.query(f"SHOW GRANTS", settings=query_settings)
            if role_mapped:
                assert role_name in r
            else:
                time.sleep(1)

    # no privilege on source table
    for instance in instances:
        assert instance.query_and_get_error(f"SELECT * FROM {src_table}", settings=query_settings) == 241

    with "privilege on source table":
        grant_select(
            cluster=cluster,
            privilege=f"SELECT ON {src_table}",
            role_or_user=role_name,
            node=node,
        )

        # user should be able to read from the source table
        for instance in instance:
                if role_mapped:
                    instance.query(f"SELECT * FROM {src_table}", settings=query_settings)
                else:
                    instance.query_and_get_error(f"SELECT * FROM {src_table}", settings=query_settings) == 241

        revoke_select(
            cluster=cluster,
            privilege=f"SELECT ON {src_table}",
            role_or_user=role_name,
            node=node,
        )

    with "privilege only on distributed table":
        grant_select(
            cluster=cluster,
            privilege=f"SELECT ON {dist_table}",
            role_or_user=role_name,
            node=node,
        )

        # user should still not be able to read from distributed table
        for instance in instances:
            instance.query_and_get_error(f"SELECT * FROM {dist_table}", settings=query_settings) == 241

        revoke_select(
            cluster=cluster,
            privilege=f"SELECT ON {dist_table}",
            role_or_user=role_name,
            node=node,
        )

    with "privilege only on source but not on distributed table":
        grant_select(
            cluster=cluster,
            privilege=f"SELECT ON {src_table}",
            role_or_user=role_name,
            node=node,
        )

        # user should still not be able to read from distributed table
        for instance in instances:
            instance.query_and_get_error(f"SELECT * FROM {dist_table}", settings=query_settings) == 241

        revoke_select(
            cluster=cluster,
            privilege=f"SELECT ON {src_table}",
            role_or_user=role_name,
            node=node,
        )

    with "privilege on source and distributed":
        grant_select(
            cluster=cluster,
            privilege=f"SELECT ON {src_table}",
            role_or_user=role_name,
            node=node,
        )

        grant_select(
            cluster=cluster,
            privilege=f"SELECT ON {dist_table}",
            role_or_user=role_name,
            node=node,
        )

        # user should be able to read from the distributed table
        for instance in instances:
            if role_mapped:
                instance.query(f"SELECT * FROM {dist_table}", settings=query_settings)
            else:
                instance.query_and_get_error(f"SELECT * FROM {dist_table}", settings=query_settings) == 241
        
        revoke_select(
            cluster=cluster,
            privilege=f"SELECT ON {src_table}",
            role_or_user=role_name,
            node=node,
        )

        revoke_select(
            cluster=cluster,
            privilege=f"SELECT ON {dist_table}",
            role_or_user=role_name,
            node=node,
        )










def execute_tests(role_name, role_mapped, ldap_user, local_user):
    """Execute all scenarios on cluster with or without secret
    for LDAP and local users, using a role that might be
    mapped or not.
    """
    for cluster_type in ["with_secret", "without_secret"]:
            for user in [ldap_user, local_user]:
                if role_mapped and user["type"] == "local user":
                    for instance in instances:
                        instance.query(f"GRANT {role_name} TO {local_user['username']}")

                outline_test_select_using_mapped_role(
                    cluster=f"sharded_cluster_{cluster_type}",
                    role_name=role_name,
                    role_mapped=role_mapped,
                    user=user
                )


def outline_using_external_user_directory(ldap_servers, mapping, ldap_roles_or_groups, rbac_roles, mapped_roles):
    """Check using simple and distributed table access when using
    LDAP external user directory or LDAP authenticated existing RBAC users
    with and without cluster secret.

    Where mapping can be one of the following:
        'static' or 'dynamic' or 'dynamic and static'
    """
    ldap_user = {
        "type": "ldap user",
        "server": "my_ldap_server",
        "username": "myuser",
        "password": "myuser",
        "dn": "cn=myuser,ou=users,dc=company,dc=com",
    }

    local_user = {
        "type": "local user",
        "username": "local_user1",
        "password": "local_user1",
    }

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
            "prefix": "clickhouse_",
        }
    ]

    if mapping in ["dynamic", "dynamic and static"]:
        # add LDAP groups
        for name in ldap_servers:
            for group_name in ldap_roles_or_groups:
                # adding {group_name}
                cluster.exec_in_container(cluster.ldap_docker_id, 
                                          add_ldap_group_command(cn=group_name), 
                                          detach=True)

                # add LDAP user to the group
                cluster.exec_in_container(cluster.ldap_docker_id, add_ldap_user_to_group_command(
                    user=ldap_user,
                    group_cn=f"cn={group_name},ou=groups,dc=company,dc=com",
                ), detach=True)

    # add LDAP external user directory configuration with {mapping} role mapping
    for name in self.context.cluster.nodes["clickhouse"]:
        if mapping == "dynamic":
            By(
                f"on node {name}",
                test=add_ldap_external_user_directory,
                parallel=True,
            )(
                server="my_ldap_server",
                role_mappings=role_mappings,
                restart=True,
                node=cluster_node(name),
            )
        elif mapping == "dynamic and static":
            By(
                f"on node {name}",
                test=add_ldap_external_user_directory,
                parallel=True,
            )(
                server="my_ldap_server",
                role_mappings=role_mappings,
                roles=ldap_roles_or_groups,
                restart=True,
                node=cluster_node(name),
            )
        else:
            By(
                f"on node {name}",
                test=add_ldap_external_user_directory,
                parallel=True,
            )(
                server="my_ldap_server",
                roles=ldap_roles_or_groups,
                restart=True,
                node=cluster_node(name),
            )

    with And("I add local RBAC user"):
        for name in self.context.cluster.nodes["clickhouse"]:
            with By(f"on node {name}"):
                add_rbac_user(user=local_user, node=cluster_node(name))

    with And("I add RBAC roles on cluster"):
        for name in self.context.cluster.nodes["clickhouse"]:
            with By(f"on node {name}"):
                add_rbac_role(roles=rbac_roles, node=cluster_node(name))

    for role_name in rbac_roles:
        execute_tests(
            role_name=role_name,
            role_mapped=(role_name in mapped_roles),
            ldap_user=ldap_user,
            local_user=local_user,
        )















def test_using_authenticated_users(started_cluster):
    """Check using simple and distributed table access when using
    LDAP authenticated existing users with and without cluster secret.
    """
    role_name = f"role_{getuid()}"

    ldap_user = {
        "type": "ldap authenticated user",
        "cn": "myuser",
        "username": "myuser",
        "userpassword": "myuser",
        "password": "myuser",
        "server": "my_ldap_server",
    }

    local_user = {
        "type": "local user",
        "username": "local_user2",
        "password": "local_user2",
    }

    # add LDAP user
    cluster.exec_in_container(cluster.ldap_docker_id, [add_user_to_ldap_command(
            cn=ldap_user["cn"], userpassword=ldap_user["userpassword"]), ], detach=True)

    # add local RBAC user
    for instance in instances:
        add_rbac_user(user=local_user, node=instance)

    # add RBAC role on cluster that user will use
    for instance in instances:
        add_rbac_role(role_name, instance)

    # create LDAP-auth user and grant role
    for instance in instances:
        instance.query(f"CREATE USER OR REPLACE {ldap_user['username']} IDENTIFIED WITH ldap SERVER '{ldap_user['server']}'")
        instance.query(f"GRANT {role_name} TO {ldap_user['username']}")

    # grant role to local RBAC user
    for instance in instances:
        instance.query(f"GRANT {role_name} TO {local_user['username']}")

    execute_tests(
        role_name=role_name,
        role_mapped=role_name,
        ldap_user=ldap_user,
        local_user=local_user,
    )


#TODO: fix up this ass
def test_using_external_user_directory():
    return