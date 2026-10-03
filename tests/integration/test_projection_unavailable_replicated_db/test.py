"""Whether a projection can be analyzed is per-replica local configuration, so the two replicas of a
`Replicated` database can disagree about it.

A `Replicated` database runs the same ALTER again on every replica. A metadata-only change must
work from either initiator and replay even where the projection cannot be analyzed, without
deleting its declaration.

Both nodes boot with `enable_positional_arguments_for_projections` in the default profile so that the
shared DDL can be analyzed on both; taking the file away from one of them is what makes them disagree.
"""

import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

POSITIONAL_XML = "/etc/clickhouse-server/users.d/positional.xml"
POSITIONAL_XML_BACKUP = "/tmp/positional.xml.bak"
POSITIONAL_XML_SOURCE = os.path.join(
    os.path.dirname(__file__), "configs/users.d/positional.xml"
)

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    user_configs=["configs/users.d/positional.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "shard1", "replica": "node1"},
)
node2 = cluster.add_instance(
    "node2",
    user_configs=["configs/users.d/positional.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "shard1", "replica": "node2"},
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

    # A metadata-only ALTER is safe even when the initiator cannot analyze the projection.
    node2.query("ALTER TABLE r.t MODIFY COMMENT 'y'")
    assert_eq_with_retry(node1, COMMENT, "y")
    assert node2.query(COMMENT).strip() == "y"
    assert node1.query(PROJECTION_COUNT).strip() == "1"
    assert "PROJECTION" in node2.query("SHOW CREATE TABLE r.t")

    # With the setting back, node2 analyzes the declaration the replayed ALTER left in place.
    node2.exec_in_container(["cp", POSITIONAL_XML_BACKUP, POSITIONAL_XML])
    node2.restart_clickhouse()
    assert node2.query(PROJECTION_COUNT).strip() == "1"


def test_replay_updates_settings_of_unavailable_projection(started_cluster):
    for replica in (node1, node2):
        replica.copy_file_to_container(POSITIONAL_XML_SOURCE, POSITIONAL_XML)
        replica.restart_clickhouse()
        replica.query("DROP DATABASE IF EXISTS r_settings SYNC")
        replica.query(
            "CREATE DATABASE r_settings ENGINE = Replicated("
            "'/test/projection_unavailable_settings', 'shard1', '{replica}')"
        )

    projection_count = (
        "SELECT count() FROM system.projections "
        "WHERE database = 'r_settings' AND table = 't'"
    )
    node1.query(
        "CREATE TABLE r_settings.t (a UInt64, b String, "
        "PROJECTION pp (SELECT b, a GROUP BY 1, 2)) "
        "ENGINE = MergeTree ORDER BY a"
    )
    assert_eq_with_retry(node2, projection_count, "1")

    node2.exec_in_container(["rm", POSITIONAL_XML])
    try:
        node2.restart_clickhouse()
        assert node2.query(projection_count).strip() == "0"

        error = node2.query_and_get_error(
            "ALTER TABLE r_settings.t MODIFY PROJECTION IF EXISTS pp "
            "(SELECT b, a GROUP BY 1, 2) WITH SETTINGS (index_granularity = 64)"
        )
        assert "Cannot modify unavailable projection" in error

        node1.query(
            "ALTER TABLE r_settings.t MODIFY PROJECTION pp "
            "(SELECT b, a GROUP BY 1, 2) WITH SETTINGS (index_granularity = 128)"
        )
        node1.query(
            "ALTER TABLE r_settings.t MODIFY PROJECTION IF EXISTS pp "
            "(SELECT b, a GROUP BY 1, 2) WITH SETTINGS (index_granularity = 256)"
        )
        node1.query("ALTER TABLE r_settings.t MODIFY COMMENT 'settings_replayed'")
        assert_eq_with_retry(
            node2,
            "SELECT comment FROM system.tables "
            "WHERE database = 'r_settings' AND name = 't'",
            "settings_replayed",
        )
        assert node2.query(projection_count).strip() == "0"
        assert "index_granularity = 256" in node2.query(
            "SHOW CREATE TABLE r_settings.t"
        )
    finally:
        node2.copy_file_to_container(POSITIONAL_XML_SOURCE, POSITIONAL_XML)
        node2.restart_clickhouse()

    assert node2.query(projection_count).strip() == "1"
    assert "index_granularity = 256" in node2.query("SHOW CREATE TABLE r_settings.t")


def test_replay_adds_projection_unavailable_on_follower(started_cluster):
    lookup_ddl = (
        "CREATE DICTIONARY default.r_add_lookup "
        "(id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() "
        "DB 'default' TABLE 'r_add_lookup_source')) LAYOUT(FLAT()) LIFETIME(0)"
    )
    for replica in (node1, node2):
        replica.query("DROP DICTIONARY IF EXISTS default.r_add_lookup")
        replica.query("DROP TABLE IF EXISTS default.r_add_lookup_source SYNC")

    def create_local_lookup(replica):
        replica.query(
            "CREATE TABLE default.r_add_lookup_source "
            "(id UInt64, value UInt64) ENGINE = Memory"
        )
        replica.query(lookup_ddl)

    create_local_lookup(node1)
    for replica in (node1, node2):
        replica.query("DROP DATABASE IF EXISTS r_add SYNC")
        replica.query(
            "CREATE DATABASE r_add ENGINE = Replicated("
            "'/test/projection_unavailable_add', 'shard1', '{replica}')"
        )

    node1.query(
        "CREATE TABLE r_add.t "
        "(a UInt64, b String, PROJECTION qq (SELECT a ORDER BY a)) "
        "ENGINE = MergeTree ORDER BY a"
    )
    assert_eq_with_retry(
        node2,
        "SELECT count() FROM system.tables WHERE database = 'r_add' AND name = 't'",
        "1",
    )

    try:
        node1.query(
            "ALTER TABLE r_add.t ADD PROJECTION pp "
            "(SELECT a, dictGet('default.r_add_lookup', 'value', a) AS d ORDER BY a) FIRST",
            settings={"distributed_ddl_task_timeout": 0},
        )
        node1.query(
            "ALTER TABLE r_add.t MODIFY COMMENT 'add_replayed'",
            settings={"distributed_ddl_task_timeout": 0},
        )
        assert_eq_with_retry(
            node2,
            "SELECT comment FROM system.tables WHERE database = 'r_add' AND name = 't'",
            "add_replayed",
        )
        assert node2.query(
            "SELECT count() FROM system.projections WHERE database = 'r_add' AND table = 't'"
        ).strip() == "1"
        restored_definition = node2.query("SHOW CREATE TABLE r_add.t")
        assert restored_definition.index("PROJECTION pp") < restored_definition.index(
            "PROJECTION qq"
        )
    finally:
        create_local_lookup(node2)
        node2.restart_clickhouse()

    assert node2.query(
        "SELECT count() FROM system.projections WHERE database = 'r_add' AND table = 't'"
    ).strip() == "2"


def test_replicated_merge_tree_replays_new_unavailable_projection(started_cluster):
    table = "default.rmt_unavailable_projection"
    projection_count = (
        "SELECT count() FROM system.projections "
        "WHERE database = 'default' AND table = 'rmt_unavailable_projection'"
    )
    persisted_projection = (
        "SELECT position(create_table_query, 'PROJECTION pp') > 0 "
        "FROM system.tables WHERE database = 'default' "
        "AND name = 'rmt_unavailable_projection'"
    )

    for replica in (node1, node2):
        replica.copy_file_to_container(POSITIONAL_XML_SOURCE, POSITIONAL_XML)
        replica.restart_clickhouse()
        replica.query(f"DROP TABLE IF EXISTS {table} SYNC")
        replica.query(
            f"CREATE TABLE {table} (a UInt64, b String) "
            "ENGINE = ReplicatedMergeTree("
            "'/test/projection_unavailable_plain_rmt', '{replica}') ORDER BY a"
        )

    node2.exec_in_container(["rm", POSITIONAL_XML])
    try:
        node2.restart_clickhouse()
        node1.query(
            f"ALTER TABLE {table} ADD PROJECTION pp "
            "(SELECT a, b GROUP BY 1, 2)"
        )

        # The leader admitted this body before publishing it to Keeper. The follower must
        # retain that accepted declaration even though its current setting cannot analyze it.
        assert_eq_with_retry(node2, persisted_projection, "1", retry_count=20, sleep_time=0.5)
        assert node2.query(projection_count).strip() == "0"
        assert node1.query(projection_count).strip() == "1"

        error = node1.query_and_get_error(
            f"ALTER TABLE {table} ADD PROJECTION bad "
            "(SELECT missing_column ORDER BY missing_column)"
        )
        assert error
        assert "PROJECTION bad" not in node1.query(f"SHOW CREATE TABLE {table}")
        assert "PROJECTION bad" not in node2.query(f"SHOW CREATE TABLE {table}")
    finally:
        node2.copy_file_to_container(POSITIONAL_XML_SOURCE, POSITIONAL_XML)
        node2.restart_clickhouse()

    assert_eq_with_retry(node2, projection_count, "1")
    for replica in (node1, node2):
        replica.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_replay_preserves_canonical_codec_body_of_unavailable_projection(started_cluster):
    for replica in (node1, node2):
        replica.copy_file_to_container(POSITIONAL_XML_SOURCE, POSITIONAL_XML)
        replica.restart_clickhouse()
        replica.query("DROP DATABASE IF EXISTS r_codec_settings SYNC")
        replica.query(
            "CREATE DATABASE r_codec_settings ENGINE = Replicated("
            "'/test/projection_unavailable_codec_settings', 'shard1', '{replica}')"
        )

    projection_count = (
        "SELECT count() FROM system.projections "
        "WHERE database = 'r_codec_settings' AND table = 't'"
    )
    node1.query(
        "CREATE TABLE r_codec_settings.t (a UInt64, "
        "PROJECTION pp (a UInt64 CODEC(Delta, ZSTD)) AS (SELECT a, count() GROUP BY 1)) "
        "ENGINE = MergeTree ORDER BY a",
        settings={"allow_projection_column_list_in_replicated_metadata": 1},
    )
    assert_eq_with_retry(node2, projection_count, "1")
    assert "Delta(8)" in node2.query("SHOW CREATE TABLE r_codec_settings.t")

    node2.exec_in_container(["rm", POSITIONAL_XML])
    try:
        node2.restart_clickhouse()
        assert node2.query(projection_count).strip() == "0"

        node1.query(
            "ALTER TABLE r_codec_settings.t MODIFY PROJECTION pp "
            "(a UInt64 CODEC(Delta, ZSTD)) AS (SELECT a, count() GROUP BY 1) "
            "WITH SETTINGS (index_granularity = 128)",
            settings={"allow_projection_column_list_in_replicated_metadata": 1},
        )
        node1.query("ALTER TABLE r_codec_settings.t MODIFY COMMENT 'codec_settings_replayed'")
        assert_eq_with_retry(
            node2,
            "SELECT comment FROM system.tables "
            "WHERE database = 'r_codec_settings' AND name = 't'",
            "codec_settings_replayed",
        )
        assert node2.query(projection_count).strip() == "0"
        restored_definition = node2.query("SHOW CREATE TABLE r_codec_settings.t")
        assert "Delta(8)" in restored_definition
        assert "index_granularity = 128" in restored_definition
    finally:
        node2.copy_file_to_container(POSITIONAL_XML_SOURCE, POSITIONAL_XML)
        node2.restart_clickhouse()

    assert node2.query(projection_count).strip() == "1"
    assert "index_granularity = 128" in node2.query("SHOW CREATE TABLE r_codec_settings.t")


def test_attach_as_replicated_rechecks_projection_column_gate(started_cluster):
    database = "attach_projection_gate"
    table = f"{database}.t"
    node1.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    node1.query(f"CREATE DATABASE {database} ENGINE = Atomic")
    node1.query(
        f"CREATE TABLE {table} (x UInt64, "
        "PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x)) "
        "ENGINE = MergeTree ORDER BY x"
    )
    node1.query(f"DETACH TABLE {table}")

    # Loading locally accepted metadata must still work with the compatibility gate off.
    node1.query(
        f"ATTACH TABLE {table}",
        settings={"allow_projection_column_list_in_replicated_metadata": 0},
    )
    node1.query(f"DETACH TABLE {table}")

    # Converting that local definition publishes it into replicated table metadata.
    error = node1.query_and_get_error(
        f"ATTACH TABLE {table} AS REPLICATED",
        settings={"allow_projection_column_list_in_replicated_metadata": 0},
    )
    assert "allow_projection_column_list_in_replicated_metadata" in error
    assert node1.query(
        "SELECT count() FROM system.tables "
        f"WHERE database = '{database}' AND name = 't'"
    ).strip() == "0"

    node1.query(
        f"ATTACH TABLE {table} AS REPLICATED",
        settings={"allow_projection_column_list_in_replicated_metadata": 1},
    )
    assert node1.query(
        "SELECT engine FROM system.tables "
        f"WHERE database = '{database}' AND name = 't'"
    ).strip() == "ReplicatedMergeTree"
    assert node1.query(
        "SELECT count() FROM system.projections "
        f"WHERE database = '{database}' AND table = 't'"
    ).strip() == "1"
    node1.query(f"DROP DATABASE {database} SYNC")


def test_replicated_database_short_attach_rechecks_projection_column_gate(started_cluster):
    database = "r_attach_projection_gate"
    table = f"{database}.t"
    for replica in (node1, node2):
        replica.query(f"DROP DATABASE IF EXISTS {database} SYNC")
        replica.query(
            f"CREATE DATABASE {database} ENGINE = Replicated("
            "'/test/projection_short_attach_gate', 'shard1', '{replica}')"
        )

    node1.query(
        f"CREATE TABLE {table} (x UInt64, "
        "PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x)) "
        "ENGINE = MergeTree ORDER BY x",
        settings={"allow_projection_column_list_in_replicated_metadata": 1},
    )
    table_count = (
        "SELECT count() FROM system.tables "
        f"WHERE database = '{database}' AND name = 't'"
    )
    assert_eq_with_retry(node2, table_count, "1")
    node1.query(f"DETACH TABLE {table} PERMANENTLY")
    assert_eq_with_retry(node1, table_count, "0")
    assert_eq_with_retry(node2, table_count, "0")

    error = node1.query_and_get_error(
        f"ATTACH TABLE {table}",
        settings={"allow_projection_column_list_in_replicated_metadata": 0},
    )
    assert "allow_projection_column_list_in_replicated_metadata" in error
    assert node1.query(table_count).strip() == "0"

    node1.query(
        f"ATTACH TABLE {table}",
        settings={"allow_projection_column_list_in_replicated_metadata": 1},
    )
    assert_eq_with_retry(node2, table_count, "1")
    for replica in (node1, node2):
        replica.query(f"DROP DATABASE {database} SYNC")
