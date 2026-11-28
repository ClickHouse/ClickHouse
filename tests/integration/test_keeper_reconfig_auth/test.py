#!/usr/bin/env python3
# Checks that Keeper dynamic reconfiguration requires authorization: with
# enable_reconfiguration=true and no client password, an unauthenticated client
# must not be able to change cluster membership. Reconfiguration operates on the
# virtual /keeper/config path, whose absent ACL used to authorize everyone.
import time

import pytest
from kazoo.exceptions import NoAuthError

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/keeper1.xml"], stay_alive=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_until_connected(cluster, node)
        yield cluster
    finally:
        cluster.shutdown()


def get_zk():
    return keeper_utils.get_fake_zk(cluster, node.name)


def test_unauthenticated_reconfiguration_is_rejected(started_cluster):
    # The probe targets a nonexistent member id, so nothing actually changes;
    # only the authorization outcome differs. An unauthenticated client is
    # rejected before the request is processed.
    zk = get_zk()
    try:
        with pytest.raises(NoAuthError):
            zk.reconfig(joining=None, leaving="9", new_members=None)
    finally:
        zk.stop()
        zk.close()

    # An ordinary digest-authenticated client (not the super identity) is also
    # rejected: the requirement is specifically for super, not for any auth.
    zk = get_zk()
    try:
        zk.add_auth("digest", "someuser:somepass")
        with pytest.raises(NoAuthError):
            zk.reconfig(joining=None, leaving="9", new_members=None)
    finally:
        zk.stop()
        zk.close()

    # A client authenticated with the configured super identity is authorized and
    # a real membership change takes effect with the gate enabled: change the
    # server's own priority and confirm the committed config reflects it.
    zk = get_zk()
    try:
        zk.add_auth("digest", "super:admin")
        zk.reconfig(
            joining="server.1=node:9234;participant;2", leaving=None, new_members=None
        )
        committed = None
        for _ in range(20):
            committed = zk.get("/keeper/config")[0]
            if b"server.1=node:9234;participant;2" in committed:
                break
            time.sleep(0.5)
        assert b"server.1=node:9234;participant;2" in committed
    finally:
        zk.stop()
        zk.close()
