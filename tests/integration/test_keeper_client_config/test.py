#!/usr/bin/env python3

import pytest
import os

from kazoo.security import make_acl

from helpers import keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/keeper_config.xml"],
    with_zookeeper=True,
    use_keeper=False,
    stay_alive=True,
)

KEEPER_PASSWORD = "foobar"
KEEPER_IDENTITY = "testuser:testpass"
KEEPER_CLIENT_CONFIG_PATH = "/tmp/keeper_client_config.xml"
KEEPER_CLIENT_CONFIG_WITH_IDENTITY_PATH = "/tmp/keeper_client_config_with_identity.xml"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        password = KEEPER_PASSWORD
        p = (password + "".join([chr(0)] * (16 - len(password)))).encode("ascii")
        keeper_utils.wait_until_connected(cluster, node1, password=p)

        # Copy keeper-client XML configs into the container
        local_config = os.path.join(
            os.path.dirname(__file__), "configs", "keeper_client_config.xml"
        )
        node1.copy_file_to_container(local_config, KEEPER_CLIENT_CONFIG_PATH)

        local_config_with_identity = os.path.join(
            os.path.dirname(__file__),
            "configs",
            "keeper_client_config_with_identity.xml",
        )
        node1.copy_file_to_container(
            local_config_with_identity, KEEPER_CLIENT_CONFIG_WITH_IDENTITY_PATH
        )

        # Create an ACL-protected node for identity tests
        fake_zk = keeper_utils.get_fake_zk(cluster, "node1", password=p)
        fake_zk.add_auth("digest", KEEPER_IDENTITY)
        fake_zk.create(
            "/test_identity_node",
            b"protected_data",
            acl=[make_acl("auth", "", all=True)],
        )
        fake_zk.stop()

        yield cluster
    finally:
        cluster.shutdown()


def test_password_from_xml_config(started_cluster):
    """Keeper-client reads password from XML config <zookeeper><password>."""
    data = node1.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse keeper-client -c {KEEPER_CLIENT_CONFIG_PATH} -q \"ls '/keeper'\"",
        ],
        privileged=True,
    )
    assert "api_version" in data


def test_password_from_env_var(started_cluster):
    """Keeper-client reads password from CLICKHOUSE_KEEPER_PASSWORD env var."""
    data = node1.exec_in_container(
        [
            "bash",
            "-c",
            "clickhouse keeper-client -h node1 -p 9181 -q \"ls '/keeper'\"",
        ],
        privileged=True,
        environment={"CLICKHOUSE_KEEPER_PASSWORD": KEEPER_PASSWORD},
    )
    assert "api_version" in data


def test_wrong_password_fails(started_cluster):
    """Keeper-client fails with a wrong password."""
    data = node1.exec_in_container(
        [
            "bash",
            "-c",
            "clickhouse keeper-client -h node1 -p 9181 --password wrong -q \"ls '/keeper'\"",
        ],
        privileged=True,
        nothrow=True,
    )
    assert "api_version" not in data


def test_no_password_fails(started_cluster):
    """Keeper-client fails without any password."""
    data = node1.exec_in_container(
        [
            "bash",
            "-c",
            "clickhouse keeper-client -h node1 -p 9181 -q \"ls '/keeper'\"",
        ],
        privileged=True,
        nothrow=True,
    )
    assert "api_version" not in data


def test_cli_password_overrides_env(started_cluster):
    """CLI --password takes priority over CLICKHOUSE_KEEPER_PASSWORD env var."""
    data = node1.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse keeper-client -h node1 -p 9181 --password {KEEPER_PASSWORD} -q \"ls '/keeper'\"",
        ],
        privileged=True,
        environment={"CLICKHOUSE_KEEPER_PASSWORD": "wrong_password"},
    )
    assert "api_version" in data


def test_identity_from_xml_config(started_cluster):
    """Keeper-client reads identity from XML config <zookeeper><identity>."""
    data = node1.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse keeper-client -c {KEEPER_CLIENT_CONFIG_WITH_IDENTITY_PATH} -q \"get '/test_identity_node'\"",
        ],
        privileged=True,
    )
    assert "protected_data" in data


def test_identity_from_env_var(started_cluster):
    """Keeper-client reads identity from CLICKHOUSE_KEEPER_IDENTITY env var."""
    data = node1.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse keeper-client -h node1 -p 9181 --password {KEEPER_PASSWORD} -q \"get '/test_identity_node'\"",
        ],
        privileged=True,
        environment={"CLICKHOUSE_KEEPER_IDENTITY": KEEPER_IDENTITY},
    )
    assert "protected_data" in data


def test_wrong_identity_fails(started_cluster):
    """Keeper-client fails to read ACL-protected node with wrong identity."""
    data = node1.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse keeper-client -h node1 -p 9181 --password {KEEPER_PASSWORD} --identity wrong:wrong -q \"get '/test_identity_node'\"",
        ],
        privileged=True,
        nothrow=True,
    )
    assert "protected_data" not in data


def test_no_identity_fails(started_cluster):
    """Keeper-client fails to read ACL-protected node without identity."""
    data = node1.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse keeper-client -c {KEEPER_CLIENT_CONFIG_PATH} -q \"get '/test_identity_node'\"",
        ],
        privileged=True,
        nothrow=True,
    )
    assert "protected_data" not in data


def test_cli_identity_overrides_env(started_cluster):
    """CLI --identity takes priority over CLICKHOUSE_KEEPER_IDENTITY env var."""
    data = node1.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse keeper-client -h node1 -p 9181 --password {KEEPER_PASSWORD} --identity {KEEPER_IDENTITY} -q \"get '/test_identity_node'\"",
        ],
        privileged=True,
        environment={"CLICKHOUSE_KEEPER_IDENTITY": "wrong:wrong"},
    )
    assert "protected_data" in data
