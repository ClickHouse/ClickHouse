#!/usr/bin/env python3
"""Regression test for keeper termination when `Get /keeper/availability_zone`
is sent to a keeper that was started without `<placement>` and runs with
`quorum_reads=true`.

Before the fix the keeper called `onStorageInconsistency` (=> `std::terminate`)
on commit, and the request entry persisted in the raft log, so log replay
re-triggered the termination on every restart.

The companion test under `quorum_reads=false` exercises the local read path
(`processLocalRequests` -> `processImpl<true>`), which already returned
`ZNONODE` gracefully before the fix. Running both ensures the two read modes
return the same response for an unconfigured AZ. A third case pins the
configured branch: with `<placement>` set, a `quorum_reads=true` `Get` should
return the AZ value normally.
"""

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
from helpers.keeper_utils import KeeperClient, KeeperException

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper_config.xml"],
    stay_alive=True,
)

node_local = cluster.add_instance(
    "node_local",
    main_configs=["configs/keeper_config_local_reads.xml"],
    stay_alive=True,
)

node_configured = cluster.add_instance(
    "node_configured",
    main_configs=["configs/keeper_config_configured.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _keeper_alive(target) -> bool:
    return keeper_utils.send_4lw_cmd(cluster, target, "ruok") == "imok"


def _assert_znonode(target, instance_name):
    keeper_utils.wait_until_connected(cluster, target)
    assert _keeper_alive(target)

    with KeeperClient.from_cluster(cluster, instance_name, port=9181) as client:
        with pytest.raises(KeeperException) as ex:
            client.get("/keeper/availability_zone")

    error = str(ex.value)
    assert "node doesn't exist" in error
    assert "/keeper/availability_zone" in error

    # The keeper should still be alive — no SIGABRT, no raft-log poisoning.
    assert _keeper_alive(target)


def test_get_availability_zone_returns_znonode_under_quorum_reads():
    """Keeper without `<placement>` should reply with `ZNONODE` for the AZ
    path even when `quorum_reads=true` routes the read through the raft
    commit path. The keeper must remain alive afterwards."""
    _assert_znonode(node, "node")


def test_get_availability_zone_returns_znonode_under_local_reads():
    """Same path, `quorum_reads=false` (the default). The read goes through
    `processLocalRequests` and `processImpl<true>` and should also return
    `ZNONODE`, matching the response of the quorum-read path after the fix."""
    _assert_znonode(node_local, "node_local")


def test_get_availability_zone_returns_value_when_configured():
    """When `<placement>` is configured, a `quorum_reads=true` `Get` must
    return the configured AZ value. This pins the configured branch of the
    fix: the container lookup still succeeds under the raft commit path."""
    keeper_utils.wait_until_connected(cluster, node_configured)
    assert _keeper_alive(node_configured)

    with KeeperClient.from_cluster(cluster, "node_configured", port=9181) as client:
        assert client.get("/keeper/availability_zone") == "az-configured"

    assert _keeper_alive(node_configured)
