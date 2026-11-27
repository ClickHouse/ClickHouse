import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1")
node2 = cluster.add_instance("node2")
node256 = cluster.add_instance(
    "node256",
    image="clickhouse/clickhouse-server",
    tag="25.6",
    with_installed_binary=True,
)
# A current server whose `default` profile disables the setting - the value is not
# compiled-in, not sent with any query, and exists only as a server-side default.
node_disabled_by_profile = cluster.add_instance(
    "node_disabled_by_profile",
    user_configs=["configs/users_packed_keys_disabled.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_string_aggregation_compatibility(started_cluster):
    def create_tables(node, other_node_name):
        node.query(
            "DROP TABLE IF EXISTS repro1 SYNC; CREATE TABLE repro1 (id UInt64, s1 String, s2 LowCardinality(String)) ENGINE = MergeTree ORDER BY id"
        )
        node.query(
            "INSERT INTO repro1 SELECT number, 'somestring', 'somestring' FROM numbers(10000)"
        )
        node.query(
            f"CREATE TABLE IF NOT EXISTS dist_repro1 (id UInt64, s1 String, s2 LowCardinality(String)) AS remote('{other_node_name}', 'default.repro1', 'default', '')"
        )
        node.query(
            "CREATE TABLE IF NOT EXISTS global_repro1 (id UInt64, s1 String, s2 LowCardinality(String)) ENGINE = Merge('default', '.*repro1')"
        )

    create_tables(node1, other_node_name=node256.name)
    create_tables(node256, other_node_name=node1.name)

    def run_query(node, extra_settings={}):
        return int(
            node.query(
                """
        SELECT count()
        FROM
        (
            SELECT
                id,
                s2
            FROM global_repro1
            GROUP BY ALL
        )""",
                settings={"group_by_two_level_threshold": 1} | extra_settings,
            )
        )

    assert run_query(node1) == 10000
    assert run_query(node256) == 10000
    # With `serialize_string_in_memory_with_zero_byte=0` the new server uses the legacy
    # serialization format. Previously this used to surface a cross-version aggregation
    # bug (returning 20000 instead of 10000) when two-level aggregation was active.
    # Starting from revision 54488, two-level aggregation is disabled when communicating
    # with older servers (see DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD),
    # which prevents the bug from being triggered in this scenario.
    assert (
        run_query(node1, extra_settings={"serialize_string_in_memory_with_zero_byte": 0})
        == 10000
    )

def test_string_aggregation_compatibility_setting(started_cluster):
    def create_tables(n, other_node_name):
        n.query(
            "DROP TABLE IF EXISTS repro2 SYNC; CREATE TABLE repro2 (id UInt64, s1 String, s2 LowCardinality(String)) ENGINE = MergeTree ORDER BY id"
        )
        n.query(
            "INSERT INTO repro2 SELECT number, 'somestring', 'somestring' FROM numbers(10000)"
        )
        n.query(
            f"CREATE TABLE IF NOT EXISTS dist_repro2 (id UInt64, s1 String, s2 LowCardinality(String)) AS remote('{other_node_name}', 'default.repro2', 'default', '')"
        )
        n.query(
            "CREATE TABLE IF NOT EXISTS global_repro2 (id UInt64, s1 String, s2 LowCardinality(String)) ENGINE = Merge('default', '.*repro2')"
        )

    create_tables(node1, other_node_name=node2.name)
    create_tables(node2, other_node_name=node1.name)

    def run_query(n, extra_settings={}):
        return int(
            n.query(
                """
        SELECT count()
        FROM
        (
            SELECT
                id,
                s2
            FROM global_repro2
            GROUP BY ALL
        )""",
                settings={"group_by_two_level_threshold": 1} | extra_settings,
            )
        )

    assert run_query(node1) == 10000
    assert run_query(node2) == 10000
    assert (
        run_query(node1, extra_settings={"serialize_string_in_memory_with_zero_byte": 0})
        == 10000
    )


def test_single_string_key_aggregation_compatibility(started_cluster):
    # Unlike the two tests above, which group by `(id, s2)` and therefore go through the
    # multi-key serialized aggregation path, this test groups by a single plain `String`
    # key. That is exactly the path `PackedStringRef` replaces (the `key_string` /
    # `key_string_two_level` variants), so it directly exercises the code this PR changes.
    #
    # The column mixes short keys that fit inline in a `PackedStringRef` (<= 11 bytes) with
    # longer keys that spill to an out-of-line pointer (> 11 bytes), covering both
    # representations. There are 1000 distinct keys, duplicated on both the new and the old
    # (25.6) server; correct cross-version two-level aggregation must deduplicate them to
    # 1000. A bucketing mismatch between versions would inflate the result (e.g. to 2000).
    def create_tables(node, other_node_name):
        node.query(
            "DROP TABLE IF EXISTS repro3 SYNC; CREATE TABLE repro3 (s String) ENGINE = MergeTree ORDER BY s"
        )
        node.query(
            "INSERT INTO repro3 SELECT concat('k', toString(number % 500)) FROM numbers(5000)"
        )
        node.query(
            "INSERT INTO repro3 SELECT concat('long_string_key_', toString(number % 500)) FROM numbers(5000)"
        )
        node.query(
            f"CREATE TABLE IF NOT EXISTS dist_repro3 (s String) AS remote('{other_node_name}', 'default.repro3', 'default', '')"
        )
        node.query(
            "CREATE TABLE IF NOT EXISTS global_repro3 (s String) ENGINE = Merge('default', '.*repro3')"
        )

    create_tables(node1, other_node_name=node256.name)
    create_tables(node256, other_node_name=node1.name)

    def run_query(node, extra_settings={}):
        return int(
            node.query(
                """
        SELECT count()
        FROM
        (
            SELECT s
            FROM global_repro3
            GROUP BY s
        )""",
                settings={"group_by_two_level_threshold": 1} | extra_settings,
            )
        )

    assert run_query(node1) == 1000
    assert run_query(node256) == 1000
    # Same legacy in-memory serialization scenario as `test_string_aggregation_compatibility`,
    # but on the single-`String` packed-key path.
    assert (
        run_query(node1, extra_settings={"serialize_string_in_memory_with_zero_byte": 0})
        == 1000
    )


def test_packed_string_keys_setting(started_cluster):
    # `enable_packed_string_keys_in_aggregation=0` falls back to the legacy
    # `StringHashTable`-based method for a single `String` key. The setting propagates
    # with the query, so distributed two-level aggregation must stay self-consistent:
    # remote shards bucket with the legacy hash and the initiator splits any stray
    # single-level blocks the same way (the merge-only `Aggregator::Params` wiring).
    def create_tables(table, node, other_node_name):
        node.query(
            f"DROP TABLE IF EXISTS {table} SYNC; CREATE TABLE {table} (s String) ENGINE = MergeTree ORDER BY s"
        )
        node.query(
            f"INSERT INTO {table} SELECT concat('k', toString(number % 500)) FROM numbers(5000)"
        )
        node.query(
            f"INSERT INTO {table} SELECT concat('long_string_key_', toString(number % 500)) FROM numbers(5000)"
        )
        node.query(
            f"CREATE TABLE IF NOT EXISTS dist_{table} (s String) AS remote('{other_node_name}', 'default.{table}', 'default', '')"
        )
        node.query(
            f"CREATE TABLE IF NOT EXISTS global_{table} (s String) ENGINE = Merge('default', '.*{table}')"
        )

    def run_query(table, node, extra_settings={}):
        return int(
            node.query(
                f"""
        SELECT count()
        FROM
        (
            SELECT s
            FROM global_{table}
            GROUP BY s
        )""",
                settings={"group_by_two_level_threshold": 1} | extra_settings,
            )
        )

    # Both servers are current: legacy two-level bucketing must agree between the
    # remote shard and the initiator.
    create_tables("repro4", node1, other_node_name=node2.name)
    create_tables("repro4", node2, other_node_name=node1.name)
    for initiator in (node1, node2):
        assert (
            run_query(
                "repro4",
                initiator,
                extra_settings={"enable_packed_string_keys_in_aggregation": 0},
            )
            == 1000
        )

    # Mixed with an old (25.6) server that does not know the setting: it is not
    # IMPORTANT, so the old server skips it with a warning, uses its own (legacy)
    # method, and the result must still be correct. Initiate from the new server
    # only - the old one would reject the unknown setting coming from the client.
    #
    # An old server cannot follow the setting, so servers below revision 54489 are
    # fenced off by DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD:
    # the initiator zeroes the two-level thresholds for them and re-buckets their
    # single-level blocks itself. The 25.6 node exercises that path for both setting
    # values. A peer that has the packed method but not the setting (a 26.8 master
    # snapshot between #110573 and this change, advertising revision 54488) takes the
    # same fenced path, but no released image pins that state, so it cannot be tested
    # directly here.
    create_tables("repro5", node1, other_node_name=node256.name)
    create_tables("repro5", node256, other_node_name=node1.name)
    for packed in (0, 1):
        assert (
            run_query(
                "repro5",
                node1,
                extra_settings={"enable_packed_string_keys_in_aggregation": packed},
            )
            == 1000
        )


def test_packed_string_keys_default_propagation(started_cluster):
    # The initiator's *effective* value of `enable_packed_string_keys_in_aggregation`
    # must reach the remote servers even when nobody mentions the setting in the query:
    # two current peers whose server-side defaults disagree would otherwise silently
    # pick different hash methods and mis-merge two-level buckets under
    # `distributed_aggregation_memory_efficient`. `MultiplexedConnections::sendQuery`
    # and `HedgedConnections::sendQuery` therefore force the setting into the changed
    # set before sending it.
    #
    # This arm discriminates exactly that path: the initiator (node1) sits on the
    # compiled-in default, which is *unchanged*, so the ordinary changed-settings
    # serialization would send nothing, while the shard's own `default` profile turns
    # the setting off (a profile default is marked changed and would propagate through
    # the ordinary path on its own, so putting the `0` on the initiator would not
    # discriminate). With the forced propagation the shard executes the secondary
    # query with the initiator's effective value (1); without it, the shard would
    # silently keep its own profile default (0). The shard-side `system.query_log`
    # entry contains the key either way, because the shard's own profile default marks
    # it changed locally, so the recorded *value* is the oracle, not the key presence.

    # Guard: the profile default is in effect on the shard.
    assert (
        node_disabled_by_profile.query(
            "SELECT value FROM system.settings WHERE name = 'enable_packed_string_keys_in_aggregation'"
        ).strip()
        == "0"
    )

    node_disabled_by_profile.query(
        "DROP TABLE IF EXISTS repro6 SYNC; CREATE TABLE repro6 (s String) ENGINE = MergeTree ORDER BY s"
    )
    node_disabled_by_profile.query(
        "INSERT INTO repro6 SELECT concat('k', toString(number % 500)) FROM numbers(5000)"
    )

    query_id = "packed_string_keys_default_propagation"
    assert (
        int(
            node1.query(
                f"""
        SELECT count()
        FROM
        (
            SELECT s
            FROM remote('{node_disabled_by_profile.name}', 'default.repro6', 'default', '')
            GROUP BY s
        )""",
                settings={"group_by_two_level_threshold": 1},
                query_id=query_id,
            )
        )
        == 500
    )

    node_disabled_by_profile.query("SYSTEM FLUSH LOGS")
    shard_side_values = node_disabled_by_profile.query(
        f"""
        SELECT DISTINCT Settings['enable_packed_string_keys_in_aggregation']
        FROM system.query_log
        WHERE type = 'QueryFinish' AND is_initial_query = 0 AND initial_query_id = '{query_id}'
        """
    ).strip()
    assert shard_side_values == "1"
