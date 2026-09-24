import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper.xml"],
    stay_alive=True,
    with_zookeeper=False,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, [node])
        yield cluster
    finally:
        cluster.shutdown()


def test_empty_multi_request(started_cluster):
    # A `Multi` request whose subrequest list holds nothing but the terminator record - what a client
    # sends when it builds a transaction from a list that turns out to be empty; kazoo serializes it
    # as exactly that for a transaction without operations. It parses, and it is preprocessed into
    # no deltas at all, so it used to be written to the changelog and then read `deltas.front()` on
    # an empty range on the raft commit thread, terminating the process - and terminating it again
    # on every restart that replayed the entry.
    zk = keeper_utils.get_fake_zk(cluster, node.name)
    try:
        # ZooKeeper answers with an empty successful multi response, which kazoo turns into an
        # empty list of results.
        assert zk.transaction().commit() == []

        assert keeper_utils.send_4lw_cmd(cluster, node, "ruok") == "imok"

        # The session and the server keep working.
        zk.create("/after_empty_multi", b"1")
        assert zk.get("/after_empty_multi")[0] == b"1"
        # A multi transaction with subrequests keeps working.
        transaction = zk.transaction()
        transaction.create("/after_empty_multi/child", b"2")
        assert transaction.commit() == ["/after_empty_multi/child"]
        assert zk.get("/after_empty_multi/child")[0] == b"2"
    finally:
        zk.stop()
        zk.close()

    # The entry of the empty multi is in the changelog, and the restart replays it.
    node.restart_clickhouse()
    keeper_utils.wait_nodes(cluster, [node])
    assert keeper_utils.send_4lw_cmd(cluster, node, "ruok") == "imok"
