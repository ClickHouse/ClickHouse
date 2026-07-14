import time
import pytest
import requests
from helpers.cluster import ClickHouseCluster
from .common import vault_startup_command

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/config_retry_vault.xml", "configs/extra_settings.xml"],
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


def test_reload_vault_failure_retried(started_cluster):
    initial_loads = int(
        instance.query(
            "SELECT value FROM system.events WHERE event = 'MainConfigLoads'"
        )
    )

    assert (
        instance.query(
            "SELECT currentUser()", user="test_user", password="test_password"
        )
        == "test_user\n"
    )

    # Delete vault secret to make the second loadConfig pass fail
    requests.delete(
        f"http://{cluster.hashicorp_vault_ip}:8200/v1/secret/metadata/username",
        headers={"X-Vault-Token": "foobar"},
    )

    # Touch a merge file to trigger background reload (the config itself
    # stays valid; only the vault secret is gone so the second pass fails).
    instance.replace_config(
        "/etc/clickhouse-server/config.d/extra_settings.xml",
        "<clickhouse></clickhouse>",
    )

    # Wait for the background reload to attempt and fail
    # (reload_interval is 1000ms, so 4 ticks is enough).
    time.sleep(4)

    # Restore vault secret
    requests.post(
        f"http://{cluster.hashicorp_vault_ip}:8200/v1/secret/data/username",
        json={"data": {"password": "test_password"}},
        headers={"X-Vault-Token": "foobar"},
    )

    # Wait for the background retry on the next periodic tick
    time.sleep(4)

    loads_after = int(
        instance.query(
            "SELECT value FROM system.events WHERE event = 'MainConfigLoads'"
        )
    )
    assert (
        loads_after > initial_loads
    ), f"MainConfigLoads did not increase ({initial_loads} -> {loads_after})"

    assert (
        instance.query(
            "SELECT currentUser()", user="test_user", password="test_password"
        )
        == "test_user\n"
    )
