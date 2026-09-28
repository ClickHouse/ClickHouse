import xml.etree.ElementTree as ET

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
    main_configs=["configs/display_secrets.xml", "configs/macros_and_queues.xml"],
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


def merge_tree_config_keys(instance):
    """The keys of the `<merge_tree>` section of the config `instance` loaded, all of its files merged."""
    preprocessed = instance.exec_in_container(
        ["cat", "/var/lib/clickhouse/preprocessed_configs/config.xml"]
    )
    section = ET.fromstring(preprocessed).find("merge_tree")
    return sorted(child.tag for child in section) if section is not None else []


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

    # Exactly what the `<merge_tree>` section assigns is attributed to it, so the reporting neither misses one
    # nor over-applies. The section is read from the config the server loaded rather than listed here, because
    # the integration harness adds keys of its own (`helpers/0_common_instance_config.xml`) to every instance.
    reported = node.query(
        "SELECT name FROM system.table_settings "
        "WHERE database = currentDatabase() AND table = 't' AND source = 'config' ORDER BY name"
    ).split()
    assert reported == merge_tree_config_keys(node)
    assert {"max_suspicious_broken_parts", "merge_max_block_size"} <= set(reported)

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
    readers = ["partial_reader", "no_display_secrets_reader", "no_collection_secrets_reader"]

    def cleanup():
        node_secrets.query("DROP TABLE IF EXISTS k SYNC")
        node_secrets.query("DROP NAMED COLLECTION IF EXISTS nc_access")
        for reader in readers:
            node_secrets.query(f"DROP USER IF EXISTS {reader}")

    cleanup()
    try:
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

        def statement_values(user):
            # `SHOW CHANGED TABLE SETTINGS` prints `name, value, changed, source`; the rows the collection supplied.
            rows = node_secrets.query(
                "SHOW CHANGED TABLE SETTINGS FROM k",
                user=user,
                settings={"format_display_secrets_in_show_and_select": 1},
            )
            return [
                row.split("\t")[:2]
                for row in rows.splitlines()
                if row.split("\t")[3] == "named_collection"
            ]

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

        # `SHOW TABLE SETTINGS` reads the same rows, so it hides them the same way.
        assert statement_values("partial_reader") == [
            ["kafka_broker_list", "[HIDDEN]"],
            ["kafka_format", "[HIDDEN]"],
            ["kafka_group_name", "[HIDDEN]"],
            ["kafka_topic_list", "[HIDDEN]"],
        ]

        # Reading the collection is not enough on its own: `system.named_collections` also asks for the secrets
        # grants, and so does this - whichever of the two a reader lacks.
        for reader, revoked in [
            ("no_display_secrets_reader", "displaySecretsInShowAndSelect ON *.*"),
            ("no_collection_secrets_reader", "SHOW NAMED COLLECTIONS SECRETS ON *"),
        ]:
            node_secrets.query(f"CREATE USER {reader} IDENTIFIED WITH no_password")
            node_secrets.query(f"GRANT ALL ON *.* TO {reader}")
            node_secrets.query(f"REVOKE {revoked} FROM {reader}")
            assert (
                node_secrets.query(
                    "SELECT count() FROM system.named_collections WHERE name = 'nc_access'", user=reader
                ).strip()
                == "1"
            ), reader
            assert collection_values(reader) == (
                "kafka_broker_list\t[HIDDEN]\n"
                "kafka_format\t[HIDDEN]\n"
                "kafka_group_name\t[HIDDEN]\n"
                "kafka_topic_list\t[HIDDEN]\n"
            ), reader

        # A reader that may read the collection reads its values, as it does in `system.named_collections`.
        assert collection_values("default") == (
            "kafka_broker_list\tsecret-broker.invalid:9092\n"
            "kafka_format\tCSV\n"
            "kafka_group_name\tsecret_group\n"
            "kafka_topic_list\tsecret_topic\n"
        )
        assert statement_values("default") == [
            ["kafka_broker_list", "secret-broker.invalid:9092"],
            ["kafka_format", "CSV"],
            ["kafka_group_name", "secret_group"],
            ["kafka_topic_list", "secret_topic"],
        ]
    finally:
        cleanup()


def test_macro_in_a_named_collection_secret_is_never_shown(started_cluster):
    """A macro from the server configuration expanded into a secret a named collection states is the server's.

    `system.named_collections` shows a reader of the collection the macro, `{nats_pw}`, not what it holds, and
    `system.table_settings` must not show that value either - to a reader with every grant, who does see the
    collection's other values. A named collection's own rule would otherwise decide, and show it.
    """
    def cleanup():
        node_secrets.query("DROP TABLE IF EXISTS n_macro SYNC")
        node_secrets.query("DROP NAMED COLLECTION IF EXISTS nc_macro")

    cleanup()
    try:
        node_secrets.query(
            "CREATE NAMED COLLECTION nc_macro AS nats_url = '127.0.0.1:1', nats_subjects = 's', "
            "nats_format = 'CSV', nats_username = 'u', nats_password = '{nats_pw}'"
        )
        node_secrets.query("CREATE TABLE n_macro (a UInt64) ENGINE = NATS(nc_macro)")

        display = {"format_display_secrets_in_show_and_select": 1}
        assert (
            node_secrets.query(
                "SELECT collection['nats_password'] FROM system.named_collections WHERE name = 'nc_macro'",
                settings=display,
            ).strip()
            == "{nats_pw}"
        )
        assert node_secrets.query(
            "SELECT name, value, is_masked, source FROM system.table_settings "
            "WHERE database = currentDatabase() AND table = 'n_macro' AND name IN ('nats_password', 'nats_username') "
            "ORDER BY name",
            settings=display,
        ) == (
            "nats_password\t[HIDDEN]\t1\tnamed_collection\n"
            "nats_username\tu\t0\tnamed_collection\n"
        )
    finally:
        cleanup()


def test_a_macro_removed_since_does_not_hide_the_table(started_cluster):
    """`NATS` expands the server's macros in its URL once, when the table is built, and keeps working if one is
    removed from the config later. Reading its settings then must not expand them again: that throws, so the table's
    rows would silently disappear, and the message - with the stated URL, credentials and all - would reach a reader
    who asked for the server's logs.
    """
    macro_file = "/etc/clickhouse-server/config.d/removable_macro.xml"

    def cleanup():
        node_secrets.query("DROP TABLE IF EXISTS n_removed_macro SYNC")
        node_secrets.exec_in_container(["rm", "-f", macro_file])
        node_secrets.query("SYSTEM RELOAD CONFIG")

    cleanup()
    try:
        node_secrets.exec_in_container(
            [
                "bash",
                "-c",
                f"echo '<clickhouse><macros><removable_host>127.0.0.1</removable_host></macros></clickhouse>' > {macro_file}",
            ]
        )
        node_secrets.query("SYSTEM RELOAD CONFIG")
        node_secrets.query(
            "CREATE TABLE n_removed_macro (a UInt64) ENGINE = NATS SETTINGS "
            "nats_url = 'nats://u:stated_pw@{removable_host}:1', nats_subjects = 's', nats_format = 'CSV'"
        )

        node_secrets.exec_in_container(["rm", "-f", macro_file])
        node_secrets.query("SYSTEM RELOAD CONFIG")
        assert node_secrets.query("SELECT count() FROM system.macros WHERE macro = 'removable_host'").strip() == "0"

        answer, logs = node_secrets.query_and_get_answer_with_error(
            "SHOW TABLE SETTINGS FROM n_removed_macro LIKE 'nats_url'",
            settings={"send_logs_level": "error", "format_display_secrets_in_show_and_select": 1},
        )
        assert answer == "nats_url\t[HIDDEN]\t1\tdefinition\n"
        assert "stated_pw" not in logs
    finally:
        cleanup()
