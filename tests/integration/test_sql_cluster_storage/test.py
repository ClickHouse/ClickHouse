import logging
import os
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
METADATA_CONFIG_CONTAINER_PATH = "/etc/clickhouse-server/config.d/cluster_metadata.xml"
SQL_CLUSTER_NAME = "sql_managed_cluster"
ON_CLUSTER = "test_cluster"
NODES = ("clickhouse1", "clickhouse2", "clickhouse3", "clickhouse4")

LOCAL_METADATA_PATHS = {
    "cluster_metadata_local.xml": "/var/lib/clickhouse/sql_clusters_metadata_local",
    "cluster_metadata_local_encrypted.xml": "/var/lib/clickhouse/sql_clusters_metadata_local_encrypted",
}

KEEPER_METADATA_PATHS = {
    "cluster_metadata_keeper.xml": "/clickhouse/sql_cluster_metadata_keeper",
    "cluster_metadata_keeper_encrypted.xml": "/clickhouse/sql_cluster_metadata_keeper_encrypted",
}

CREATE_CLUSTER_QUERY = f"""
CREATE CLUSTER {SQL_CLUSTER_NAME} ON CLUSTER '{ON_CLUSTER}' (
    user = 'default',
    password = 'secret',
    SHARD (
        REPLICA (host = 'clickhouse1', port = 9000),
        REPLICA (host = 'clickhouse2', port = 9000)
    ),
    SHARD (
        REPLICA (host = 'clickhouse3', port = 9000),
        REPLICA (host = 'clickhouse4', port = 9000)
    )
)
"""

ALTER_CLUSTER_QUERY = f"""
ALTER CLUSTER {SQL_CLUSTER_NAME} ON CLUSTER '{ON_CLUSTER}' (
    user = 'default',
    password = 'secret',
    SHARD (
        REPLICA (host = 'clickhouse1', port = 9000),
        REPLICA (host = 'clickhouse2', port = 9000),
        REPLICA (host = 'clickhouse3', port = 9000)
    ),
    SHARD (
        REPLICA (host = 'clickhouse4', port = 9000)
    )
)
"""

EXPECTED_INITIAL = """\
sql_managed_cluster\t1\t1\tclickhouse1\t9000
sql_managed_cluster\t1\t2\tclickhouse2\t9000
sql_managed_cluster\t2\t1\tclickhouse3\t9000
sql_managed_cluster\t2\t2\tclickhouse4\t9000"""

EXPECTED_ALTERED = """\
sql_managed_cluster\t1\t1\tclickhouse1\t9000
sql_managed_cluster\t1\t2\tclickhouse2\t9000
sql_managed_cluster\t1\t3\tclickhouse3\t9000
sql_managed_cluster\t2\t1\tclickhouse4\t9000"""


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        common_kwargs = dict(
            main_configs=["configs/config.d/cluster.xml"],
            user_configs=["configs/users.d/users.xml"],
            with_zookeeper=True,
            stay_alive=True,
        )
        for name in NODES:
            cluster.add_instance(name, **common_kwargs)

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def cleanup_local_metadata(nodes, metadata_path):
    for node in nodes:
        node.exec_in_container(["bash", "-c", f"rm -rf '{metadata_path}'"])


def cleanup_keeper_metadata(zk, keeper_path):
    if zk.exists(keeper_path):
        for child in zk.get_children(keeper_path):
            zk.delete(f"{keeper_path}/{child}")


def install_metadata_storage(cluster, config_file, *, local_metadata_path=None, keeper_metadata_path=None):
    nodes = list(cluster.instances.values())

    if local_metadata_path is not None:
        cleanup_local_metadata(nodes, local_metadata_path)
    if keeper_metadata_path is not None:
        cleanup_keeper_metadata(cluster.get_kazoo_client("zoo1"), keeper_metadata_path)

    host_config_path = os.path.join(SCRIPT_DIR, "configs/config.d", config_file)
    with open(host_config_path, "r", encoding="utf-8") as config:
        config_contents = config.read()

    for node in nodes:
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"cat > {METADATA_CONFIG_CONTAINER_PATH} <<'EOF'\n{config_contents}\nEOF",
            ]
        )
        node.restart_clickhouse()


def drop_sql_cluster(node, on_cluster):
    if on_cluster:
        node.query(f"DROP CLUSTER IF EXISTS {SQL_CLUSTER_NAME} ON CLUSTER '{ON_CLUSTER}'")
    else:
        node.query(f"DROP CLUSTER IF EXISTS {SQL_CLUSTER_NAME}")


def clusters_query():
    return (
        "SELECT cluster, shard_num, replica_num, host_name, port "
        f"FROM system.clusters WHERE cluster = '{SQL_CLUSTER_NAME}' "
        "ORDER BY shard_num, replica_num FORMAT TSV"
    )


def assert_cluster_state(nodes, expected):
    for node in nodes:
        assert expected == node.query(clusters_query()).strip()


def wait_cluster_state(nodes, expected, timeout=30):
    """Wait until every node observes the expected SQL-managed cluster topology.

    For Keeper-backed metadata, non-initiator replicas rely on the background root-version
    watch (not ON CLUSTER DDL) to pick up CREATE/ALTER/DROP.
    """
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            assert_cluster_state(nodes, expected)
            return
        except AssertionError as error:
            last_error = error
            time.sleep(0.5)
    raise AssertionError(f"Cluster state did not converge within {timeout}s: {last_error}")


def wait_cluster_absent(nodes, timeout=30):
    deadline = time.time() + timeout
    last_counts = None
    while time.time() < deadline:
        last_counts = [
            node.query(
                f"SELECT count() FROM system.clusters WHERE cluster = '{SQL_CLUSTER_NAME}'"
            ).strip()
            for node in nodes
        ]
        if all(count == "0" for count in last_counts):
            return
        time.sleep(0.5)
    raise AssertionError(f"Cluster was not dropped on all nodes within {timeout}s: {last_counts}")


def read_local_metadata_file(node, metadata_path):
    metadata_dir = metadata_path.removeprefix("/var/lib/clickhouse/")
    file_path = os.path.join(node.path, "database", metadata_dir, f"{SQL_CLUSTER_NAME}.sql")
    with open(file_path, "rb") as metadata_file:
        return metadata_file.read()


def check_local_metadata(nodes, metadata_path, encrypted):
    for node in nodes:
        content = read_local_metadata_file(node, metadata_path)
        if encrypted:
            assert content[:3] == b"ENC"
            assert b"secret" not in content
            assert b"clickhouse1" not in content
        else:
            assert content[:3] != b"ENC"
            assert b"clickhouse1" in content
            assert b"secret" in content


def check_keeper_metadata(zk, keeper_path, encrypted):
    zk.sync(keeper_path)
    children = zk.get_children(keeper_path)
    assert f"{SQL_CLUSTER_NAME}.sql" in children
    content = zk.get(f"{keeper_path}/{SQL_CLUSTER_NAME}.sql")[0]
    if encrypted:
        assert content[:3] == b"ENC"
        assert b"secret" not in content
        assert b"clickhouse1" not in content
    else:
        assert content[:3] != b"ENC"
        assert b"clickhouse1" in content
        assert b"secret" in content


def run_storage_scenario(cluster, *, config_file, use_on_cluster, encrypted, keeper):
    local_metadata_path = LOCAL_METADATA_PATHS.get(config_file)
    keeper_metadata_path = KEEPER_METADATA_PATHS.get(config_file)

    install_metadata_storage(
        cluster,
        config_file,
        local_metadata_path=local_metadata_path,
        keeper_metadata_path=keeper_metadata_path,
    )

    nodes = [cluster.instances[name] for name in NODES]
    leader = nodes[0]
    zk = cluster.get_kazoo_client("zoo1") if keeper else None

    for node in nodes:
        drop_sql_cluster(node, use_on_cluster)

    if use_on_cluster:
        leader.query(CREATE_CLUSTER_QUERY)
    else:
        leader.query(CREATE_CLUSTER_QUERY.replace(f" ON CLUSTER '{ON_CLUSTER}'", ""))

    if keeper or not use_on_cluster:
        wait_cluster_state(nodes, EXPECTED_INITIAL)
    else:
        assert_cluster_state(nodes, EXPECTED_INITIAL)
    if keeper:
        check_keeper_metadata(zk, keeper_metadata_path, encrypted)
    else:
        check_local_metadata(nodes, local_metadata_path, encrypted)

    for node in nodes:
        node.restart_clickhouse()
    assert_cluster_state(nodes, EXPECTED_INITIAL)

    if use_on_cluster:
        nodes[1].query(ALTER_CLUSTER_QUERY)
    else:
        # Single-node ALTER: only the initiator writes Keeper; other replicas must pick up the
        # change via the root-version data watch (children-list watches miss ALTER).
        nodes[1].query(ALTER_CLUSTER_QUERY.replace(f" ON CLUSTER '{ON_CLUSTER}'", ""))

    if keeper or not use_on_cluster:
        wait_cluster_state(nodes, EXPECTED_ALTERED)
    else:
        assert_cluster_state(nodes, EXPECTED_ALTERED)
    if keeper:
        check_keeper_metadata(zk, keeper_metadata_path, encrypted)
    else:
        check_local_metadata(nodes, local_metadata_path, encrypted)

    if use_on_cluster:
        nodes[2].query(f"DROP CLUSTER {SQL_CLUSTER_NAME} ON CLUSTER '{ON_CLUSTER}'")
    else:
        nodes[2].query(f"DROP CLUSTER {SQL_CLUSTER_NAME}")

    if keeper or not use_on_cluster:
        wait_cluster_absent(nodes)
    else:
        for node in nodes:
            assert "0" == node.query(
                f"SELECT count() FROM system.clusters WHERE cluster = '{SQL_CLUSTER_NAME}'"
            ).strip()

    if keeper:
        zk.sync(keeper_metadata_path)
        assert SQL_CLUSTER_NAME + ".sql" not in zk.get_children(keeper_metadata_path)


def test_local_storage_on_cluster(cluster):
    run_storage_scenario(
        cluster,
        config_file="cluster_metadata_local.xml",
        use_on_cluster=True,
        encrypted=False,
        keeper=False,
    )


def test_local_storage_encrypted_on_cluster(cluster):
    run_storage_scenario(
        cluster,
        config_file="cluster_metadata_local_encrypted.xml",
        use_on_cluster=True,
        encrypted=True,
        keeper=False,
    )


def test_keeper_storage(cluster):
    run_storage_scenario(
        cluster,
        config_file="cluster_metadata_keeper.xml",
        use_on_cluster=False,
        encrypted=False,
        keeper=True,
    )


def test_keeper_storage_encrypted(cluster):
    run_storage_scenario(
        cluster,
        config_file="cluster_metadata_keeper_encrypted.xml",
        use_on_cluster=False,
        encrypted=True,
        keeper=True,
    )


def test_keeper_drop_removes_stale_cluster_when_reload_cannot_materialize(cluster):
    """A reload that cannot materialize a shared definition must still track the installed cluster.

    Replica B loads `c`. Replica A then stores a definition B cannot construct (`secure = 1` with
    no `port`, and B has no `tcp_port_secure`). If tracking were cleared before that throw, A's
    subsequent `DROP CLUSTER` would leave B serving the deleted topology (and its secret) forever,
    and `DROP CLUSTER IF EXISTS` on B could not repair it.
    """
    keeper_path = KEEPER_METADATA_PATHS["cluster_metadata_keeper.xml"]
    install_metadata_storage(
        cluster,
        "cluster_metadata_keeper.xml",
        keeper_metadata_path=keeper_path,
    )

    node_a = cluster.instances["clickhouse1"]
    node_b = cluster.instances["clickhouse2"]
    name = "reload_fail_cluster"
    count_query = f"SELECT count() FROM system.clusters WHERE cluster = '{name}'"
    topology_query = (
        f"SELECT host_name, port FROM system.clusters WHERE cluster = '{name}' "
        "ORDER BY replica_num FORMAT TSV"
    )

    node_a.query(f"DROP CLUSTER IF EXISTS {name}")
    try:
        node_a.query(f"CREATE CLUSTER {name} (SHARD (REPLICA (host = 'clickhouse1', port = 9000)))")
        assert_eq_with_retry(node_b, count_query, "1\n")
        assert node_b.query(topology_query).strip() == "clickhouse1\t9000"

        # Same shape as `ALTER` on a replica that has `tcp_port_secure` while B does not:
        # `secure = 1` with no `port` makes `Cluster::Address` look up `tcp_port_secure`, which
        # is absent here, so `materializeCluster` throws `NO_ELEMENTS_IN_CONFIG`.
        zk = cluster.get_kazoo_client("zoo1")
        unmaterializable = (
            f"CREATE CLUSTER {name} (SHARD (REPLICA (host = 'clickhouse1', secure = 1)))"
        )
        zk.set(f"{keeper_path}/{name}.sql", unmaterializable.encode())
        root_data, _ = zk.get(keeper_path)
        zk.set(keeper_path, root_data)

        node_b.wait_for_log_line("Port is not specified in cluster configuration")
        assert node_b.query(topology_query).strip() == "clickhouse1\t9000"

        node_a.query(f"DROP CLUSTER {name}")
        assert_eq_with_retry(node_b, count_query, "0\n")

        node_b.query(f"DROP CLUSTER IF EXISTS {name}")
        assert node_b.query(count_query).strip() == "0"
    finally:
        node_a.query(f"DROP CLUSTER IF EXISTS {name}")
        node_b.query(f"DROP CLUSTER IF EXISTS {name}")


def test_keeper_drop_other_cluster_while_reload_cannot_materialize(cluster):
    """A definition this replica cannot materialize must not block `DROP` of a different cluster.

    B loads `a` and `b`. A stores a definition of `b` that B cannot construct (`secure = 1`, no
    `port`) and then drops `a`. If B materializes before applying confirmed deletions, every reload
    throws on `b` and B keeps serving `a` (and its secret).
    """
    keeper_path = KEEPER_METADATA_PATHS["cluster_metadata_keeper.xml"]
    install_metadata_storage(
        cluster,
        "cluster_metadata_keeper.xml",
        keeper_metadata_path=keeper_path,
    )

    node_a = cluster.instances["clickhouse1"]
    node_b = cluster.instances["clickhouse2"]
    dropped = "reload_drop_other_cluster"
    kept = "reload_keep_unmaterializable_cluster"
    dropped_count = f"SELECT count() FROM system.clusters WHERE cluster = '{dropped}'"
    kept_count = f"SELECT count() FROM system.clusters WHERE cluster = '{kept}'"
    kept_topology = (
        f"SELECT host_name, port FROM system.clusters WHERE cluster = '{kept}' "
        "ORDER BY replica_num FORMAT TSV"
    )

    for name in (dropped, kept):
        node_a.query(f"DROP CLUSTER IF EXISTS {name}")
    try:
        node_a.query(f"CREATE CLUSTER {dropped} (SHARD (REPLICA (host = 'clickhouse1', port = 9000)))")
        node_a.query(f"CREATE CLUSTER {kept} (SHARD (REPLICA (host = 'clickhouse1', port = 9000)))")
        assert_eq_with_retry(node_b, dropped_count, "1\n")
        assert_eq_with_retry(node_b, kept_count, "1\n")
        assert node_b.query(kept_topology).strip() == "clickhouse1\t9000"

        zk = cluster.get_kazoo_client("zoo1")
        unmaterializable = (
            f"CREATE CLUSTER {kept} (SHARD (REPLICA (host = 'clickhouse1', secure = 1)))"
        )
        zk.set(f"{keeper_path}/{kept}.sql", unmaterializable.encode())
        root_data, _ = zk.get(keeper_path)
        zk.set(keeper_path, root_data)

        node_b.wait_for_log_line("Port is not specified in cluster configuration")
        assert node_b.query(dropped_count).strip() == "1"
        assert node_b.query(kept_topology).strip() == "clickhouse1\t9000"

        node_a.query(f"DROP CLUSTER {dropped}")
        assert_eq_with_retry(node_b, dropped_count, "0\n")
        assert node_b.query(kept_count).strip() == "1"
        assert node_b.query(kept_topology).strip() == "clickhouse1\t9000"
    finally:
        for name in (dropped, kept):
            node_a.query(f"DROP CLUSTER IF EXISTS {name}")
            node_b.query(f"DROP CLUSTER IF EXISTS {name}")


def test_keeper_drop_keeps_config_cluster_with_same_name(cluster):
    """A SQL cluster created on one node is skipped on a node which has a configured cluster with the same name.
    `DROP CLUSTER` on that node removes the shared metadata but must keep its configured cluster."""
    install_metadata_storage(
        cluster,
        "cluster_metadata_keeper.xml",
        keeper_metadata_path=KEEPER_METADATA_PATHS["cluster_metadata_keeper.xml"],
    )

    node_sql = cluster.instances["clickhouse1"]
    node_config = cluster.instances["clickhouse2"]
    name = "collision_cluster"
    config_path = "/etc/clickhouse-server/config.d/collision_cluster.xml"

    node_config.exec_in_container(
        [
            "bash",
            "-c",
            f"cat > {config_path} <<'EOF'\n"
            f"<clickhouse><remote_servers><{name}><shard><replica><host>clickhouse2</host><port>9000</port></replica></shard></{name}></remote_servers></clickhouse>\n"
            "EOF",
        ]
    )
    node_config.query("SYSTEM RELOAD CONFIG")

    sources_query = f"SELECT DISTINCT source FROM system.clusters WHERE cluster = '{name}'"
    try:
        assert node_config.query(sources_query).strip() == "CONFIG"

        node_sql.query(f"CREATE CLUSTER {name} (SHARD (REPLICA (host = 'clickhouse1', port = 9000)))")
        assert node_sql.query(sources_query).strip() == "SQL"
        node_config.wait_for_log_line(f"Not loading SQL cluster `{name}`")
        assert node_config.query(sources_query).strip() == "CONFIG"

        node_config.query(f"DROP CLUSTER {name}")
        assert node_config.query(sources_query).strip() == "CONFIG"
        assert node_config.query(f"SELECT count() FROM cluster({name}, system.one)").strip() == "1"

        assert_eq_with_retry(node_sql, sources_query, "")
    finally:
        node_sql.query(f"DROP CLUSTER IF EXISTS {name}")
        node_config.exec_in_container(["bash", "-c", f"rm -f {config_path}"])
        node_config.query("SYSTEM RELOAD CONFIG")


def test_create_query_secrets_require_grant(cluster):
    """`system.clusters.create_query` reveals the cluster `secret` and passwords only with every condition met:
    the server setting (enabled in this fixture), the session setting and the `displaySecretsInShowAndSelect` grant."""
    node = cluster.instances["clickhouse1"]
    name = "secrets_cluster"
    user = "secrets_reader"

    node.query(f"DROP CLUSTER IF EXISTS {name}")
    node.query(f"DROP USER IF EXISTS {user}")
    node.query(
        f"CREATE CLUSTER {name} (secret = 'cluster_secret_value', password = 'cluster_password_value', "
        "SHARD (host = 'clickhouse1', port = 9000, password = 'shard_password_value'), "
        "SHARD (REPLICA (host = 'clickhouse2', port = 9000, password = 'replica_password_value')))"
    )
    node.query(f"CREATE USER {user}")
    node.query(f"GRANT SELECT ON system.clusters TO {user}")

    query = f"SELECT DISTINCT create_query FROM system.clusters WHERE cluster = '{name}'"
    secrets = ("cluster_secret_value", "cluster_password_value", "shard_password_value", "replica_password_value")
    try:
        for settings in ({"format_display_secrets_in_show_and_select": 0}, {"format_display_secrets_in_show_and_select": 1}):
            create_query = node.query(query, user=user, settings=settings)
            assert "[HIDDEN]" in create_query
            assert not any(secret in create_query for secret in secrets)

        node.query(f"GRANT displaySecretsInShowAndSelect ON *.* TO {user}")

        create_query = node.query(query, user=user, settings={"format_display_secrets_in_show_and_select": 0})
        assert not any(secret in create_query for secret in secrets)

        create_query = node.query(query, user=user, settings={"format_display_secrets_in_show_and_select": 1})
        assert all(secret in create_query for secret in secrets)
    finally:
        node.query(f"DROP CLUSTER IF EXISTS {name}")
        node.query(f"DROP USER IF EXISTS {user}")


def rewrite_hosts_alias(node, alias, ip):
    """Rewrite `/etc/hosts` in place: the file is bind-mounted, so `sed -i` fails with EBUSY."""
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"grep -v -w '{alias}' /etc/hosts > /tmp/hosts && "
            f"echo '{ip} {alias}' >> /tmp/hosts && "
            "cat /tmp/hosts > /etc/hosts && rm /tmp/hosts",
        ],
        user="root",
    )


def remove_hosts_alias(node, alias):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"grep -v -w '{alias}' /etc/hosts > /tmp/hosts && "
            "cat /tmp/hosts > /etc/hosts && rm /tmp/hosts",
        ],
        user="root",
    )


def test_sql_cluster_locality_recomputed_after_drop_dns_cache(cluster):
    """A SQL replica whose hostname moves from loopback to a remote IP must stop being local
    after `SYSTEM DROP DNS CACHE`. Local metadata has no periodic reload that would hide a
    stale `ClusterPtr` copied across the DNS rebuild."""
    install_metadata_storage(
        cluster,
        "cluster_metadata_local.xml",
        local_metadata_path=LOCAL_METADATA_PATHS["cluster_metadata_local.xml"],
    )

    node = cluster.instances["clickhouse1"]
    peer = cluster.instances["clickhouse2"]
    name = "dns_locality_cluster"
    alias = "sql_cluster_dns_host"
    table = "dns_locality_t"
    dist = "dns_locality_d"

    def is_local():
        return node.query(
            f"SELECT is_local FROM system.clusters WHERE cluster = '{name}'"
        ).strip()

    try:
        rewrite_hosts_alias(node, alias, "127.0.0.1")
        node.query("SYSTEM DROP DNS CACHE")
        node.query(f"DROP CLUSTER IF EXISTS {name}")
        node.query(f"CREATE CLUSTER {name} (SHARD (host = '{alias}', port = 9000))")
        assert is_local() == "1"

        for instance in (node, peer):
            instance.query(f"DROP TABLE IF EXISTS {table}")
            instance.query(f"CREATE TABLE {table} (x UInt8) ENGINE = Memory")
        node.query(f"DROP TABLE IF EXISTS {dist}")
        node.query(
            f"CREATE TABLE {dist} AS {table} ENGINE = Distributed({name}, default, {table})"
        )

        rewrite_hosts_alias(node, alias, peer.ip_address)
        assert is_local() == "1", "DNS cache should keep the replica local until it is dropped"

        node.query("SYSTEM DROP DNS CACHE")
        assert is_local() == "0"

        node.query(
            f"INSERT INTO {dist} SETTINGS prefer_localhost_replica = 1, "
            "distributed_foreground_insert = 1 VALUES (1)"
        )
        assert node.query(f"SELECT count() FROM {table}").strip() == "0"
        assert peer.query(f"SELECT count() FROM {table}").strip() == "1"
    finally:
        node.query(f"DROP TABLE IF EXISTS {dist}")
        for instance in (node, peer):
            instance.query(f"DROP TABLE IF EXISTS {table}")
        node.query(f"DROP CLUSTER IF EXISTS {name}")
        remove_hosts_alias(node, alias)
        node.query("SYSTEM DROP DNS CACHE")
