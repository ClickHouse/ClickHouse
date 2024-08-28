"""Test Interserver responses on configured IP."""

import pytest
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/cluster.xml", "configs/config.d/s3.xml"],
    with_minio=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


# The same value as in ClickHouse, this can't be confugured via config now
DEFAULT_RESOLVE_TIME_HISTORY_SECONDS = 2 * 60


def test_host_resolver(start_cluster):
    minio_ip = cluster.get_instance_ip("minio1")

    # drop DNS cache
    node.set_hosts(
        [
            (minio_ip, "minio1"),
            (node.ip_address, "minio1"),  # no answer on 9001 port on this IP
        ]
    )

    node.query("SYSTEM DROP DNS CACHE")
    node.query("SYSTEM DROP CONNECTIONS CACHE")

    node.query(
        """
                CREATE TABLE test (key UInt32, value UInt32)
                Engine=MergeTree()
                ORDER BY key PARTITION BY key
                SETTINGS storage_policy='s3'
                """
    )

    initial_fails = "0\n"
    k = 0
    limit = 100
    while initial_fails == "0\n":
        node.query(
            f"""
                    INSERT INTO test VALUES (0,{k})
                    """
        )
        # HostResolver chooses IP randomly, so on single call can choose worked ID
        initial_fails = node.query(
            "SELECT value FROM system.events WHERE event LIKE 'AddressesMarkedAsFailed'"
        )
        k += 1
        if k >= limit:
            # Dead IP was not choosen for 100 iteration.
            # This is not expected, but not an error actually.
            # And test should be stopped.
            return

    # initial_fails can be more than 1 if clickhouse does something in several parallel threads

    for j in range(10):
        for i in range(10):
            node.query(
                f"""
                        INSERT INTO test VALUES ({i+1},{j+1})
                        """
            )
            fails = node.query(
                "SELECT value FROM system.events WHERE event LIKE 'AddressesMarkedAsFailed'"
            )
            assert fails == initial_fails

    # Check that clickhouse tries to recheck IP after 2 minutes
    time.sleep(DEFAULT_RESOLVE_TIME_HISTORY_SECONDS)

    intermediate_fails = initial_fails
    limit = k + 100
    while intermediate_fails == initial_fails:
        node.query(
            f"""
                    INSERT INTO test VALUES (101,{k})
                    """
        )
        intermediate_fails = node.query(
            "SELECT value FROM system.events WHERE event LIKE 'AddressesMarkedAsFailed'"
        )
        k += 1
        if k >= limit:
            # Dead IP was not choosen for 100 iteration.
            # This is not expected, but not an error actually.
            # And test should be stopped.
            return

    # After another 2 minutes shoudl not be new fails, next retry after 4 minutes
    time.sleep(DEFAULT_RESOLVE_TIME_HISTORY_SECONDS)

    initial_fails = intermediate_fails
    limit = k + 100
    while intermediate_fails == initial_fails:
        node.query(
            f"""
                    INSERT INTO test VALUES (102,{k})
                    """
        )
        intermediate_fails = node.query(
            "SELECT value FROM system.events WHERE event LIKE 'AddressesMarkedAsFailed'"
        )
        k += 1
        if k >= limit:
            break

    assert k == limit
