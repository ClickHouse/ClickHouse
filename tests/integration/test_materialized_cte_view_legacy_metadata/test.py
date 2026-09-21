# Materialized CTEs in materialized view definitions (issue 113711).
# 1. A stored definition that fixes `enable_global_with_statement` (legacy metadata, which a fresh CREATE
#    now rejects) keeps its `MATERIALIZED` CTE references when loaded by short ATTACH and at server start,
#    so the CTE is materialized once; the server logs a warning about the fixed setting.
# 2. In a Replicated database, a CREATE or MODIFY QUERY committed by an older initiator that fixes the
#    setting is replayed on an upgraded replica instead of failing, while the upgraded replica rejects
#    such a command of its own.
# 3. A view with a plain CTE named like a table records no dependency on that table after a restart.
import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# `with_remote_database_disk=False`: `test_legacy_metadata_loads_and_materializes` edits a metadata file on the local disk.
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

# Both references are in the declaring SELECT, which keeps working with the setting fixed to 0.
DEFINITION = (
    "WITH r_legacy AS MATERIALIZED (SELECT id, rand64() AS x FROM {db}.src) "
    "SELECT a.id AS id, a.x = b.x AS same FROM r_legacy AS a INNER JOIN r_legacy AS b ON a.id = b.id"
)
CLAUSE = " SETTINGS enable_global_with_statement = 0"
WARNING = "declares a MATERIALIZED CTE"
# The feature is experimental and off by default: an insert that fires the view has to enable it, and it
# cannot go into `DEFINITION`'s own `SETTINGS` clause, which the `sed` of the first test appends to.
MATERIALIZED_CTE_ON = {"enable_materialized_cte": 1}


def warning_lines(instance):
    return instance.grep_in_log(WARNING).count("\n")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def prepare(instance, db):
    # `r_legacy` is a real table with the CTE's name and columns: rows with ids 0..6 in `dst` would prove
    # that a reference bound to the table instead of the CTE.
    instance.query(f"DROP TABLE IF EXISTS {db}.src SYNC")
    instance.query(f"DROP TABLE IF EXISTS {db}.r_legacy SYNC")
    instance.query(f"DROP TABLE IF EXISTS {db}.dst SYNC")
    instance.query(f"CREATE TABLE {db}.src (id UInt32) ENGINE = MergeTree ORDER BY id")
    instance.query(f"INSERT INTO {db}.src VALUES (1), (2), (3)")
    instance.query(f"CREATE TABLE {db}.r_legacy (id UInt32, x UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query(f"INSERT INTO {db}.r_legacy SELECT number, number FROM numbers(7)")
    instance.query(f"CREATE TABLE {db}.dst (id UInt32, same UInt8) ENGINE = MergeTree ORDER BY id")


def test_legacy_metadata_loads_and_materializes(started_cluster):
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
    warnings_after_attach = warning_lines(node)
    assert warnings_after_attach >= 1
    node.query("INSERT INTO default.src VALUES (81), (82)", settings=MATERIALIZED_CTE_ON)
    # The references are kept, so both read one materialization (same = 1), and only the inserted block is read.
    assert node.query("SELECT * FROM default.dst ORDER BY id") == "81\t1\n82\t1\n"

    # Server start loads the same metadata through createTableFromAST.
    node.restart_clickhouse()
    assert warning_lines(node) > warnings_after_attach
    node.query("INSERT INTO default.src VALUES (83)", settings=MATERIALIZED_CTE_ON)
    assert node.query("SELECT * FROM default.dst ORDER BY id") == "81\t1\n82\t1\n83\t1\n"

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

    # An older initiator commits a CREATE with the clause; the upgraded replica replays it.
    old.query("CREATE MATERIALIZED VIEW rdb.mv_old TO rdb.dst AS " + DEFINITION.format(db="rdb") + CLAUSE)
    node.query("SYSTEM SYNC DATABASE REPLICA rdb")
    assert "enable_global_with_statement = 0" in node.query("SHOW CREATE TABLE rdb.mv_old")
    node.query("INSERT INTO rdb.src VALUES (81), (82)", settings=MATERIALIZED_CTE_ON)
    assert node.query("SELECT * FROM rdb.dst ORDER BY id") == "81\t1\n82\t1\n"

    # The same for MODIFY QUERY committed by the older initiator on a view created without the clause.
    node.query("CREATE MATERIALIZED VIEW rdb.mv_modified TO rdb.dst AS SELECT id, 1 AS same FROM rdb.src WHERE id > 1000")
    old.query("SYSTEM SYNC DATABASE REPLICA rdb")
    old.query("ALTER TABLE rdb.mv_modified MODIFY QUERY " + DEFINITION.format(db="rdb") + CLAUSE)
    node.query("SYSTEM SYNC DATABASE REPLICA rdb")
    assert "enable_global_with_statement = 0" in node.query("SHOW CREATE TABLE rdb.mv_modified")
    node.query("INSERT INTO rdb.src VALUES (91), (92)", settings=MATERIALIZED_CTE_ON)
    # Both views fire now (mv_old and mv_modified), both keeping their references.
    assert node.query("SELECT id, same, count() FROM rdb.dst WHERE id > 90 GROUP BY id, same ORDER BY id") == "91\t1\t2\n92\t1\t2\n"

    # The upgraded replica rejects its own MODIFY QUERY with the clause before enqueueing it.
    with pytest.raises(QueryRuntimeException, match="NOT_IMPLEMENTED"):
        node.query("ALTER TABLE rdb.mv_modified MODIFY QUERY SELECT id, 1 AS same FROM rdb.src" + CLAUSE)
    old.query("SYSTEM SYNC DATABASE REPLICA rdb")
    assert "r_legacy" in old.query("SHOW CREATE TABLE rdb.mv_modified")

    for instance in (node, old):
        instance.query("DROP DATABASE IF EXISTS rdb SYNC")


def test_plain_cte_dependency_after_restart(started_cluster):
    # The table loader rebuilds the referential graph from the raw metadata, where a CTE reference is a
    # bare name; a plain CTE named like a table must not turn into a dependency on it.
    for name in ("default.v_plain_cte", "default.plain_src", "default.c_plain"):
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.query("CREATE TABLE default.plain_src (id UInt32) ENGINE = MergeTree ORDER BY id")
    node.query("CREATE TABLE default.c_plain (id UInt32) ENGINE = MergeTree ORDER BY id")
    node.query("CREATE VIEW default.v_plain_cte AS WITH c_plain AS (SELECT id FROM default.plain_src) SELECT * FROM c_plain")

    node.restart_clickhouse()

    node.query("DROP TABLE default.c_plain SETTINGS check_referential_table_dependencies = 1")
    with pytest.raises(QueryRuntimeException, match="HAVE_DEPENDENT_OBJECTS"):
        node.query("DROP TABLE default.plain_src SETTINGS check_referential_table_dependencies = 1")
    node.query("DROP TABLE default.v_plain_cte SYNC")
    node.query("DROP TABLE default.plain_src SYNC")
