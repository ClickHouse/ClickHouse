import logging
import os
import time

import pytest

from helpers.cluster import ClickHouseCluster

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
        nodes[1].query(ALTER_CLUSTER_QUERY.replace(f" ON CLUSTER '{ON_CLUSTER}'", ""))

    if keeper:
        time.sleep(5)

    assert_cluster_state(nodes, EXPECTED_ALTERED)
    if keeper:
        check_keeper_metadata(zk, keeper_metadata_path, encrypted)
    else:
        check_local_metadata(nodes, local_metadata_path, encrypted)

    if use_on_cluster:
        nodes[2].query(f"DROP CLUSTER {SQL_CLUSTER_NAME} ON CLUSTER '{ON_CLUSTER}'")
    else:
        nodes[2].query(f"DROP CLUSTER {SQL_CLUSTER_NAME}")

    if keeper:
        time.sleep(5)

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
