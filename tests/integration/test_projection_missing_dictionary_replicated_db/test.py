"""Whether a projection can be analyzed is per-replica local configuration, so the two replicas of a
`Replicated` database can disagree about it.

A `Replicated` database runs the same ALTER again on every replica. Only the initiator decides whether
the change is allowed; refusing it again on a replica whose own dictionary happens to be missing would
stop the database's DDL queue and leave the replicas with different metadata. This test pins both
halves: the replay is not refused, and it still does not delete the declaration.
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

DICT_XML = "/etc/clickhouse-server/dictionaries/proj_dict.xml"
DICT_XML_BACKUP = "/tmp/proj_dict.xml.bak"
DICT_CSV = "/var/lib/clickhouse/user_files/proj_dict.csv"

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    dictionaries=["configs/dictionaries/proj_dict.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"replica": "node1"},
)
node2 = cluster.add_instance(
    "node2",
    dictionaries=["configs/dictionaries/proj_dict.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"replica": "node2"},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def projections(node):
    return node.query(
        "SELECT count() FROM system.projections WHERE database = 'r' AND table = 't'"
    ).strip()


def active_projection_parts(node):
    return node.query(
        "SELECT count() FROM system.projection_parts"
        " WHERE database = 'r' AND table = 't' AND active"
    ).strip()


def test_replay_on_replica_keeps_the_declaration(started_cluster):
    for node in (node1, node2):
        node.exec_in_container(
            ["bash", "-c", f"printf 'key,val\\n0,100\\n1,101\\n' > {DICT_CSV}"]
        )
        node.exec_in_container(["cp", DICT_XML, DICT_XML_BACKUP])
        node.query("DROP DATABASE IF EXISTS r SYNC")
        node.query(
            "CREATE DATABASE r ENGINE = Replicated('/test/projection_missing_dict', 'shard1', '{replica}')"
        )

    # A plain MergeTree table: a `Replicated` database replicates its metadata, which is the surface
    # this test is about.
    node1.query(
        "CREATE TABLE r.t (x UInt64,"
        " PROJECTION p (SELECT x, dictGet('proj_dict', 'val', x % 2) AS dv GROUP BY x, dv))"
        " ENGINE = MergeTree ORDER BY x"
    )
    node1.query("INSERT INTO r.t SELECT number FROM numbers(100)")

    assert projections(node1) == "1"
    assert_eq_with_retry(node2, "SELECT count() FROM system.projections WHERE database = 'r' AND table = 't'", "1")

    # Only node2 loses the dictionary, so after its restart the replicas genuinely disagree about
    # whether the declaration can be analyzed.
    node2.exec_in_container(["rm", DICT_XML])
    node2.restart_clickhouse()
    assert projections(node2) == "0"
    assert projections(node1) == "1"

    # The healthy initiator's ALTER must go through, which means the replay on node2 was not refused:
    # a refusal there would stop the DDL queue.
    node1.query("ALTER TABLE r.t MODIFY COMMENT 'x'")
    assert_eq_with_retry(
        node2, "SELECT comment FROM system.tables WHERE database = 'r' AND name = 't'", "x"
    )
    assert "PROJECTION" in node2.query("SHOW CREATE TABLE r.t")

    # The mirror case: node2 is the initiator of its own ALTER, so there the refusal does apply, and
    # nothing may reach node1.
    error = node2.query_and_get_error("ALTER TABLE r.t MODIFY COMMENT 'y'")
    assert "could not be analyzed" in error
    assert (
        node1.query(
            "SELECT comment FROM system.tables WHERE database = 'r' AND name = 't'"
        ).strip()
        == "x"
    )

    node2.exec_in_container(["cp", DICT_XML_BACKUP, DICT_XML])
    node2.restart_clickhouse()
    assert projections(node2) == "1"
    assert active_projection_parts(node2) == active_projection_parts(node1)
