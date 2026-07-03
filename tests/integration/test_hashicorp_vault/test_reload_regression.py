import os
import pytest
from helpers.cluster import ClickHouseCluster
from .common import vault_startup_command

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/config_token.xml"],
    user_configs=["configs/users.xml"],
    with_hashicorp_vault=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.set_hashicorp_vault_startup_command(vault_startup_command)
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _restore_vault_config():
    host_config = os.path.join(
        instance.path, "configs", "config.d", "config_token.xml"
    )
    instance.copy_file_to_container(
        host_config,
        "/etc/clickhouse-server/config.d/config_token.xml",
    )
    try:
        instance.query("SYSTEM RELOAD CONFIG")
    except Exception:
        pass


def test_reload_removed_vault_fails_closed(started_cluster):
    _restore_vault_config()

    assert (
        instance.query(
            "SELECT currentUser()", user="test_user", password="test_password"
        )
        == "test_user\n"
    )

    instance.replace_config(
        "/etc/clickhouse-server/config.d/config_token.xml",
        "<clickhouse></clickhouse>",
    )

    error = instance.query_and_get_error("SYSTEM RELOAD CONFIG")
    assert "vault is not loaded" in error

    assert (
        instance.query(
            "SELECT currentUser()", user="test_user", password="test_password"
        )
        == "test_user\n"
    )


def test_reload_broken_vault_fails_closed(started_cluster):
    _restore_vault_config()

    assert (
        instance.query(
            "SELECT currentUser()", user="test_user", password="test_password"
        )
        == "test_user\n"
    )

    instance.replace_in_config(
        "/etc/clickhouse-server/config.d/config_token.xml",
        "http://hashicorpvault:8200",
        "http://nonexistent:9999",
    )

    error = instance.query_and_get_error("SYSTEM RELOAD CONFIG")
    assert "Exception" in error

    assert (
        instance.query(
            "SELECT currentUser()", user="test_user", password="test_password"
        )
        == "test_user\n"
    )
