import time
import pytest
from helpers.cluster import ClickHouseCluster
from .common import vault_startup_command_cert

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/config_cert_reload.xml"],
    user_configs=["configs/users.xml"],
    with_hashicorp_vault=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.set_hashicorp_vault_startup_command(vault_startup_command_cert)
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_reload_ssl_paths_rotated(started_cluster):
    assert (
        instance.query(
            "SELECT currentUser()", user="test_user", password="test_password"
        )
        == "test_user\n"
    )

    initial_loads = int(
        instance.query(
            "SELECT value FROM system.events WHERE event = 'MainConfigLoads'",
            user="test_user",
            password="test_password",
        )
    )

    # Touch a vault SSL cert file on disk.  With the fix, this path is in
    # extra_paths so the background reloader detects the mtime change and
    # triggers a config reload, which increments MainConfigLoads.
    instance.exec_in_container(
        ["touch", "/etc/clickhouse-server/config.d/client.crt"]
    )

    # Wait for at least two background ticks (reload_interval is 1000ms).
    time.sleep(4)

    loads_after = int(
        instance.query(
            "SELECT value FROM system.events WHERE event = 'MainConfigLoads'",
            user="test_user",
            password="test_password",
        )
    )
    assert (
        loads_after > initial_loads
    ), f"MainConfigLoads did not increase ({initial_loads} -> {loads_after}): vault SSL file rotation did not trigger reload"

    assert (
        instance.query(
            "SELECT currentUser()", user="test_user", password="test_password"
        )
        == "test_user\n"
    )
