# Materialized CTEs in materialized view definitions (issue 113711).
# 1. A stored definition that fixes `enable_global_with_statement` (legacy metadata, which a fresh CREATE
#    now rejects) keeps the legacy full expansion when loaded by short ATTACH and at server start.
# 2. In a Replicated database, a CREATE or MODIFY QUERY committed by an older initiator that fixes the
#    setting is replayed on an upgraded replica with the legacy full expansion instead of failing, while
#    the upgraded replica rejects such a command of its own before enqueueing it.
import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# `with_remote_database_disk=False`: `test_legacy_metadata_is_expanded_on_load` edits a metadata file on the local disk.
node = cluster.add_instance(
    "node",
    with_zookeeper=True,
    macros={"shard": 1, "replica": 1},
    stay_alive=True,
    with_remote_database_disk=False,
)
old = cluster.add_instance(
    "old",
    with_zookeeper=True,
    macros={"shard": 1, "replica": 2},
    image="clickhouse/clickhouse-server",
    tag="26.5",
    with_installed_binary=True,
    stay_alive=True,
    with_remote_database_disk=False,
)

DEFINITION = (
    "WITH r_legacy AS MATERIALIZED (SELECT id, rand64() AS x FROM {db}.src) "
    "SELECT a.id AS id, a.x = b.x AS same FROM (SELECT id, x FROM r_legacy) AS a INNER JOIN r_legacy AS b ON a.id = b.id"
)
CLAUSE = " SETTINGS enable_global_with_statement = 0"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def prepare(instance, db):
    # `r_legacy` is a real table with the CTE's name and columns: rows with ids 1..3 in `dst` would prove
    # that the nested reference bound to the table instead of the CTE.
    instance.query(f"DROP TABLE IF EXISTS {db}.src SYNC")
    instance.query(f"DROP TABLE IF EXISTS {db}.r_legacy SYNC")
    instance.query(f"DROP TABLE IF EXISTS {db}.dst SYNC")
    instance.query(f"CREATE TABLE {db}.src (id UInt32) ENGINE = MergeTree ORDER BY id")
    instance.query(f"INSERT INTO {db}.src VALUES (1), (2), (3)")
    instance.query(f"CREATE TABLE {db}.r_legacy (id UInt32, x UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query(f"INSERT INTO {db}.r_legacy SELECT number, number FROM numbers(7)")
    instance.query(f"CREATE TABLE {db}.dst (id UInt32, same UInt8) ENGINE = MergeTree ORDER BY id")


def test_legacy_metadata_is_expanded_on_load(started_cluster):
    prepare(node, "default")
    node.query("DROP TABLE IF EXISTS default.mv_legacy SYNC")
    node.query("CREATE MATERIALIZED VIEW default.mv_legacy TO default.dst AS " + DEFINITION.format(db="default"))
    node.query("DETACH TABLE default.mv_legacy SYNC")
    # Turn the stored definition into what an older server accepted: append the clause to the metadata.
    node.exec_in_container(
        ["bash", "-c", r"sed -i '$ s/$/ SETTINGS enable_global_with_statement = 0/' /var/lib/clickhouse/metadata/default/mv_legacy.sql"],
        user="root",
    )

    # Short ATTACH rewrites the stored metadata: loaded, not fresh.
    node.query("ATTACH TABLE default.mv_legacy")
    assert "enable_global_with_statement = 0" in node.query("SHOW CREATE TABLE default.mv_legacy")
    node.query("INSERT INTO default.src VALUES (81), (82)")
    # Legacy expansion: the CTE is inlined at both references (same = 0), and only the inserted block is read.
    assert node.query("SELECT * FROM default.dst ORDER BY id") == "81\t0\n82\t0\n"

    # Server start loads the same metadata through createTableFromAST.
    node.restart_clickhouse()
    node.query("INSERT INTO default.src VALUES (83)")
    assert node.query("SELECT * FROM default.dst ORDER BY id") == "81\t0\n82\t0\n83\t0\n"

    node.query("DROP TABLE default.mv_legacy SYNC")


def test_replicated_replay_from_older_initiator(started_cluster):
    for instance in (node, old):
        instance.query("DROP DATABASE IF EXISTS rdb SYNC")
    node.query("CREATE DATABASE rdb ENGINE = Replicated('/clickhouse/rdb', '{shard}', '{replica}')")
    old.query("CREATE DATABASE rdb ENGINE = Replicated('/clickhouse/rdb', '{shard}', '{replica}')")
    prepare(node, "rdb")
    old.query("SYSTEM SYNC DATABASE REPLICA rdb")

    # An upgraded initiator rejects the clause before enqueueing: nothing reaches the other replica.
    with pytest.raises(QueryRuntimeException, match="NOT_IMPLEMENTED"):
        node.query("CREATE MATERIALIZED VIEW rdb.mv_new TO rdb.dst AS " + DEFINITION.format(db="rdb") + CLAUSE)
    old.query("SYSTEM SYNC DATABASE REPLICA rdb")
    assert old.query("SELECT count() FROM system.tables WHERE database = 'rdb' AND name = 'mv_new'") == "0\n"

    # An older initiator commits a CREATE with the clause; the upgraded replica replays it with the legacy expansion.
    old.query("CREATE MATERIALIZED VIEW rdb.mv_old TO rdb.dst AS " + DEFINITION.format(db="rdb") + CLAUSE)
    node.query("SYSTEM SYNC DATABASE REPLICA rdb")
    assert "enable_global_with_statement = 0" in node.query("SHOW CREATE TABLE rdb.mv_old")
    node.query("INSERT INTO rdb.src VALUES (81), (82)")
    assert node.query("SELECT * FROM rdb.dst ORDER BY id") == "81\t0\n82\t0\n"

    # The same for MODIFY QUERY committed by the older initiator on a view created without the clause.
    node.query("CREATE MATERIALIZED VIEW rdb.mv_modified TO rdb.dst AS SELECT id, 1 AS same FROM rdb.src WHERE id > 1000")
    old.query("SYSTEM SYNC DATABASE REPLICA rdb")
    old.query("ALTER TABLE rdb.mv_modified MODIFY QUERY " + DEFINITION.format(db="rdb") + CLAUSE)
    node.query("SYSTEM SYNC DATABASE REPLICA rdb")
    assert "enable_global_with_statement = 0" in node.query("SHOW CREATE TABLE rdb.mv_modified")
    node.query("INSERT INTO rdb.src VALUES (91), (92)")
    # Both views fire now (mv_old and mv_modified), both with the legacy expansion.
    assert node.query("SELECT id, same, count() FROM rdb.dst WHERE id > 90 GROUP BY id, same ORDER BY id") == "91\t0\t2\n92\t0\t2\n"

    # The upgraded replica rejects its own MODIFY QUERY with the clause before enqueueing it.
    with pytest.raises(QueryRuntimeException, match="NOT_IMPLEMENTED"):
        node.query("ALTER TABLE rdb.mv_modified MODIFY QUERY SELECT id, 1 AS same FROM rdb.src" + CLAUSE)
    old.query("SYSTEM SYNC DATABASE REPLICA rdb")
    assert "r_legacy" in old.query("SHOW CREATE TABLE rdb.mv_modified")

    for instance in (node, old):
        instance.query("DROP DATABASE IF EXISTS rdb SYNC")
