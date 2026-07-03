"""Test that ClickHouse starts when an Azure disk is configured but unreachable.

Reproduces the issue where a transport-level failure during Azure container
existence check in `getContainerClient` would crash the server at startup.
"""

import os

import pytest

from helpers.cluster import ClickHouseCluster

NODE_NAME = "node"

# A port on localhost that nothing listens on — connection refused is instant,
# unlike an unresolvable hostname which blocks on DNS timeout.
# Must be outside the CI port pool range (30000–50000) defined in helpers/cluster.py.
UNREACHABLE_PORT = 60111

DISK_CONFIG = f"""<clickhouse>
    <storage_configuration>
        <disks>
            <unreachable_azure_disk>
                <type>azure_blob_storage</type>
                <storage_account_url>http://localhost:{UNREACHABLE_PORT}/devstoreaccount1</storage_account_url>
                <container_name>cont</container_name>
                <account_name>devstoreaccount1</account_name>
                <account_key>Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==</account_key>
                <skip_access_check>true</skip_access_check>
                <max_tries>0</max_tries>
                <retry_initial_backoff_ms>1</retry_initial_backoff_ms>
                <retry_max_backoff_ms>1</retry_max_backoff_ms>
            </unreachable_azure_disk>
        </disks>
        <policies>
            <unreachable_azure_policy>
                <volumes>
                    <main>
                        <disk>unreachable_azure_disk</disk>
                    </main>
                </volumes>
            </unreachable_azure_policy>
        </policies>
    </storage_configuration>
</clickhouse>
"""

CONFIG_PATH = "/etc/clickhouse-server/config.d/unreachable_azure.xml"


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            NODE_NAME,
            stay_alive=True,
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_server_starts_with_unreachable_azure_disk(cluster):
    """Server must start even if an Azure disk endpoint is unreachable."""
    node = cluster.instances[NODE_NAME]

    # Inject the unreachable Azure disk config and restart.
    node.replace_config(CONFIG_PATH, DISK_CONFIG)
    node.restart_clickhouse(stop_start_wait_sec=30)

    assert node.query("SELECT 1").strip() == "1"

    # Clean up for next test.
    node.exec_in_container(["bash", "-c", f"rm -f {CONFIG_PATH}"])
    node.restart_clickhouse(stop_start_wait_sec=30)
