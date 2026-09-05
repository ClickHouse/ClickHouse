"""Whether a projection can be analyzed is per-replica local configuration, so the two replicas of a
`Replicated` database can disagree about it.

A `Replicated` database runs the same ALTER again on every replica. Only the initiator decides whether the
change is allowed; refusing it again on a replica whose own configuration cannot analyze the projection
would stop the database's DDL queue and leave the replicas with different metadata. This test pins both
halves: the replay is not refused, and it still does not delete the declaration.

Both nodes boot with `enable_positional_arguments_for_projections` in the default profile so that the
shared DDL can be analyzed on both; taking the file away from one of them is what makes them disagree.
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

POSITIONAL_XML = "/etc/clickhouse-server/users.d/positional.xml"
POSITIONAL_XML_BACKUP = "/tmp/positional.xml.bak"

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    user_configs=["configs/users.d/positional.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"replica": "node1"},
)
node2 = cluster.add_instance(
    "node2",
    user_configs=["configs/users.d/positional.xml"],
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


PROJECTION_COUNT = (
    "SELECT count() FROM system.projections WHERE database = 'r' AND table = 't'"
)
COMMENT = "SELECT comment FROM system.tables WHERE database = 'r' AND name = 't'"


def test_replay_on_replica_keeps_the_declaration(started_cluster):
    for node in (node1, node2):
        node.exec_in_container(["cp", POSITIONAL_XML, POSITIONAL_XML_BACKUP])
        node.query("DROP DATABASE IF EXISTS r SYNC")
        node.query(
            "CREATE DATABASE r ENGINE = Replicated('/test/projection_unavailable', 'shard1', '{replica}')"
        )

    # A plain MergeTree table: a `Replicated` database replicates its metadata, which is the surface this
    # test is about.
    node1.query(
        "CREATE TABLE r.t (a UInt64, b String,"
        " PROJECTION pp (SELECT b, a GROUP BY 1, 2))"
        " ENGINE = MergeTree ORDER BY a"
    )
    node1.query("INSERT INTO r.t SELECT number, toString(number) FROM numbers(100)")

    assert node1.query(PROJECTION_COUNT).strip() == "1"
    assert_eq_with_retry(node2, PROJECTION_COUNT, "1")

    # Only node2 loses the setting, so after its restart the replicas genuinely disagree about whether
    # the declaration can be analyzed.
    node2.exec_in_container(["rm", POSITIONAL_XML])
    node2.restart_clickhouse()
    assert node2.query(PROJECTION_COUNT).strip() == "0"
    assert node1.query(PROJECTION_COUNT).strip() == "1"

    # The healthy initiator's ALTER must go through, which means the replay on node2 was not refused: a
    # refusal there would stop the DDL queue. Do not compare the two statements byte for byte, the
    # preserved declaration is appended rather than put back where it was.
    node1.query("ALTER TABLE r.t MODIFY COMMENT 'x'")
    assert_eq_with_retry(node2, COMMENT, "x")
    assert "PROJECTION" in node2.query("SHOW CREATE TABLE r.t")

    # The mirror case: node2 is the initiator of its own ALTER, so there the refusal does apply, and
    # nothing may reach node1.
    error = node2.query_and_get_error("ALTER TABLE r.t MODIFY COMMENT 'y'")
    assert "could not be analyzed" in error
    assert node1.query(COMMENT).strip() == "x"

    # With the setting back, node2 analyzes the declaration the replayed ALTER left in place.
    node2.exec_in_container(["cp", POSITIONAL_XML_BACKUP, POSITIONAL_XML])
    node2.restart_clickhouse()
    assert node2.query(PROJECTION_COUNT).strip() == "1"
