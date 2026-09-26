import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# `system.table_settings.source` for settings a server config section assigns. Most of that is covered
# by `05058_settings_source_attribution`, which runs `clickhouse-local` with a config file. What needs a
# real server with Keeper is the `ReplicatedMergeTree` family, which reads its own
# `<replicated_merge_tree>` section into a separate cached baseline. The exact set of settings reported
# as `config` is checked here too, because a server's config includes the harness's own sections.
node = cluster.add_instance(
    "node",
    main_configs=["configs/merge_tree_settings.xml"],
    with_zookeeper=True,
    # `test_config_source_survives_a_restart` restarts it.
    stay_alive=True,
)

# `compatibility` rolls the `MergeTree` baseline back to an older release's defaults. `05058` covers that through
# `clickhouse-local`; a server is what has a default profile, which is where the baseline reads it from - the rule
# this pull request states in `Context::getMergeTreeSettings`.
node_compat = cluster.add_instance(
    "node_compat",
    user_configs=["configs/compatibility.xml"],
)


# What a named collection supplied is shown only to a reader who may read that collection, which needs a server
# where secrets can be displayed at all - the stateless test server does not enable
# `display_secrets_in_show_and_select`, so `05243` can only assert that values stay hidden there.
node_secrets = cluster.add_instance(
    "node_secrets",
    main_configs=["configs/display_secrets.xml"],
    # `default` has to be able to make the collection and to grant what the reader is given.
    user_configs=["configs/named_collection_admin.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def source_of(setting, table="t"):
    return node.query(
        f"SELECT source FROM system.table_settings "
        f"WHERE database = currentDatabase() AND table = '{table}' AND name = '{setting}'"
    ).strip()


def test_config_assignment_is_reported(started_cluster):
    node.query("DROP TABLE IF EXISTS t SYNC")
    node.query("CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x")

    # A setting the config does not mention at all.
    assert source_of("merge_max_block_size_bytes") == "default"

    # Nothing beyond what a config section assigned is attributed to one, so the reporting does not
    # over-apply. Two of these come from this test's config and two from the integration harness's
    # own `helpers/0_common_instance_config.xml`, which sets them for every instance.
    assert node.query(
        "SELECT name FROM system.table_settings "
        "WHERE database = currentDatabase() AND table = 't' AND source = 'config' ORDER BY name"
    ).split() == [
        "max_suspicious_broken_parts",
        "merge_max_block_size",
        "vertical_merge_algorithm_min_columns_to_activate",
        "vertical_merge_algorithm_min_rows_to_activate",
    ]

    node.query("DROP TABLE t SYNC")


def test_config_assignment_is_reported_for_replicated(started_cluster):
    # The replicated family reads an additional config section and has its own cached baseline, so
    # it is attributed separately and needs its own coverage.
    node.query("DROP TABLE IF EXISTS tr SYNC")
    node.query(
        "CREATE TABLE tr (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tr', '1') ORDER BY x"
    )

    assert source_of("merge_max_block_size", "tr") == "config"
    assert source_of("merge_max_block_size_bytes", "tr") == "default"

    # A setting the `<replicated_merge_tree>` section alone assigns: the replicated baseline must carry it,
    # and the plain one must not, which is what keeps the two baselines and their sources apart.
    assert source_of("max_replicated_merges_in_queue", "tr") == "config"
    assert (
        node.query(
            "SELECT value FROM system.table_settings WHERE database = currentDatabase() "
            "AND table = 'tr' AND name = 'max_replicated_merges_in_queue'"
        ).strip()
        == "77"
    )

    node.query("DROP TABLE IF EXISTS t_plain SYNC")
    node.query("CREATE TABLE t_plain (x UInt64) ENGINE = MergeTree ORDER BY x")
    assert source_of("max_replicated_merges_in_queue", "t_plain") == "default"
    node.query("DROP TABLE t_plain SYNC")

    node.query("DROP TABLE tr SYNC")



def test_compatibility_is_reported(started_cluster):
    # A setting `compatibility` rolled back is not the engine's compiled-in default and nothing in the table's
    # own definition put it there, so neither `default` nor `definition` would be the truth about it.
    node_compat.query("DROP TABLE IF EXISTS tc SYNC")
    node_compat.query("CREATE TABLE tc (x UInt64) ENGINE = MergeTree ORDER BY x")

    rolled_back = node_compat.query(
        "SELECT name, value != `default` FROM system.table_settings WHERE database = currentDatabase() "
        "AND table = 'tc' AND source = 'compatibility' ORDER BY name"
    ).splitlines()
    # The exact set is whatever `23.3` changed since, so the assertion is on the rule, not on the list: every
    # such setting holds a value that is not the compiled-in default, and there is at least one of them.
    assert rolled_back, "compatibility = 23.3 rolled nothing back"
    assert all(line.endswith("\t1") for line in rolled_back), rolled_back

    # And a server whose profile does not set it attributes none of its settings that way.
    node.query("DROP TABLE IF EXISTS t_no_compat SYNC")
    node.query("CREATE TABLE t_no_compat (x UInt64) ENGINE = MergeTree ORDER BY x")
    assert (
        node.query(
            "SELECT count() FROM system.table_settings WHERE database = currentDatabase() "
            "AND table = 't_no_compat' AND source = 'compatibility'"
        ).strip()
        == "0"
    )
    node.query("DROP TABLE t_no_compat SYNC")

    node_compat.query("DROP TABLE tc SYNC")


def test_config_source_survives_a_restart(started_cluster):
    # A restart loads the table from what it stored, and the `SETTINGS` clause stored there says nothing about
    # the config section - the server's baseline does, and the table has to take it from there again.
    node.query("DROP TABLE IF EXISTS t_restart SYNC")
    node.query("CREATE TABLE t_restart (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 4096")

    assert source_of("max_suspicious_broken_parts", "t_restart") == "config"
    assert source_of("index_granularity", "t_restart") == "definition"

    node.restart_clickhouse()

    assert source_of("max_suspicious_broken_parts", "t_restart") == "config"
    assert source_of("index_granularity", "t_restart") == "definition"
    assert (
        node.query(
            "SELECT value FROM system.table_settings WHERE database = currentDatabase() "
            "AND table = 't_restart' AND name = 'max_suspicious_broken_parts'"
        ).strip()
        == "7"
    )

    node.query("DROP TABLE t_restart SYNC")


def test_named_collection_values_need_access_to_that_collection(started_cluster):
    """A reader that may not see a collection in `system.named_collections` must not read its values here.

    `system.named_collections` asks two things: whether this reader may see the collection, granted per
    collection, and whether it may see secrets. A table built on a collection has to ask both, of the collection
    that supplied the value - otherwise a user holding only the secrets grant reads, through the settings of a
    table, a collection that table's own `SHOW CREATE` would not name.
    """
    node_secrets.query("DROP TABLE IF EXISTS k SYNC")
    node_secrets.query("DROP NAMED COLLECTION IF EXISTS nc_access")
    node_secrets.query("DROP USER IF EXISTS partial_reader")

    node_secrets.query(
        "CREATE NAMED COLLECTION nc_access AS kafka_broker_list = 'secret-broker.invalid:9092', "
        "kafka_topic_list = 'secret_topic', kafka_group_name = 'secret_group', kafka_format = 'CSV'"
    )
    node_secrets.query("CREATE TABLE k (a UInt64) ENGINE = Kafka(nc_access)")

    def collection_values(user):
        return node_secrets.query(
            "SELECT name, value FROM system.table_settings "
            "WHERE database = currentDatabase() AND table = 'k' AND source = 'named_collection' "
            "ORDER BY name",
            user=user,
            settings={"format_display_secrets_in_show_and_select": 1},
        )

    # Everything is granted, except seeing this one collection.
    node_secrets.query("CREATE USER partial_reader IDENTIFIED WITH no_password")
    node_secrets.query("GRANT ALL ON *.* TO partial_reader")
    node_secrets.query("REVOKE SHOW NAMED COLLECTIONS ON nc_access FROM partial_reader")

    assert (
        node_secrets.query(
            "SELECT count() FROM system.named_collections WHERE name = 'nc_access'",
            user="partial_reader",
        ).strip()
        == "0"
    )
    assert collection_values("partial_reader") == (
        "kafka_broker_list\t[HIDDEN]\n"
        "kafka_format\t[HIDDEN]\n"
        "kafka_group_name\t[HIDDEN]\n"
        "kafka_topic_list\t[HIDDEN]\n"
    )

    # A reader that may read the collection reads its values, as it does in `system.named_collections`.
    assert collection_values("default") == (
        "kafka_broker_list\tsecret-broker.invalid:9092\n"
        "kafka_format\tCSV\n"
        "kafka_group_name\tsecret_group\n"
        "kafka_topic_list\tsecret_topic\n"
    )

    node_secrets.query("DROP USER partial_reader")
    node_secrets.query("DROP TABLE k SYNC")
    node_secrets.query("DROP NAMED COLLECTION nc_access")
