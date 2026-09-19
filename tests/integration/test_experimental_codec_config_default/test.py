"""An experimental codec (`ZXC`) set as a `<merge_tree>` config default must not enter tables
of sessions that did not set `enable_zxc_codec`, neither on CREATE, nor on load
(short `ATTACH`), nor via `ALTER TABLE ... RESET SETTING` falling back to the config default.

The default profile has `enable_zxc_codec = 1` (the legitimate way for an operator
to opt in to such a config default), and the tests run individual queries with the setting
disabled to exercise the gate.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/merge_tree.xml"],
    user_configs=["configs/enable_zxc_codec.xml"],
    stay_alive=True,
)
# Same config default, but the default profile does NOT opt in: a session-level
# opt-in alone must not let the config default into a table, because the value is
# not stored in the table metadata and the table would fail to load on restart,
# when it is re-validated against the default profile.
node_profile_disabled = cluster.add_instance(
    "node_profile_disabled",
    main_configs=["configs/merge_tree.xml"],
)
# The default profile opts in, but `system_profile` is a separate profile that does not. The load
# context of `TablesLoader` is a copy of the global context, whose settings are the `system_profile`
# snapshot, so the metadata-load path must not check the config-inherited codec against it - only
# against the default profile, which is the durable server policy.
node_system_profile_disabled = cluster.add_instance(
    "node_system_profile_disabled",
    main_configs=["configs/merge_tree.xml", "configs/system_profile.xml"],
    user_configs=["configs/default_profile_opt_in_system_profile_disabled.xml"],
    stay_alive=True,
)

DISABLED = {"enable_zxc_codec": 0}
ENABLED = {"enable_zxc_codec": 1}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_create_inheriting_config_default_is_gated(started_cluster):
    error = node.query_and_get_error(
        "CREATE TABLE t_create (x UInt64) ENGINE = MergeTree ORDER BY x",
        settings=DISABLED,
    )
    assert "experimental" in error

    node.query(
        "CREATE TABLE t_create (x UInt64) ENGINE = MergeTree ORDER BY x",
        settings=ENABLED,
    )
    node.query("DROP TABLE t_create SYNC")

    # An explicit non-experimental override makes the config default irrelevant.
    node.query(
        "CREATE TABLE t_create (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS default_compression_codec = 'LZ4'",
        settings=DISABLED,
    )
    node.query("DROP TABLE t_create SYNC")


def test_reset_setting_to_config_default_is_gated(started_cluster):
    node.query(
        "CREATE TABLE t_reset (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS default_compression_codec = 'LZ4'",
        settings=DISABLED,
    )

    error = node.query_and_get_error(
        "ALTER TABLE t_reset RESET SETTING default_compression_codec",
        settings=DISABLED,
    )
    assert "experimental" in error

    node.query(
        "ALTER TABLE t_reset RESET SETTING default_compression_codec",
        settings=ENABLED,
    )
    node.query("DROP TABLE t_reset SYNC")


def test_attach_inheriting_config_default_is_gated(started_cluster):
    # The table stores no codec settings, so on every load the value falls back
    # to the current config default and must be re-validated.
    node.query(
        "CREATE TABLE t_attach (x UInt64) ENGINE = MergeTree ORDER BY x",
        settings=ENABLED,
    )
    node.query("DETACH TABLE t_attach")

    error = node.query_and_get_error("ATTACH TABLE t_attach", settings=DISABLED)
    assert "experimental" in error

    node.query("ATTACH TABLE t_attach", settings=ENABLED)
    node.query("DROP TABLE t_attach SYNC")


def test_attach_with_stored_codec_setting_is_exempt(started_cluster):
    # A codec stored in the table's own SETTINGS clause was gated when it was
    # introduced, so re-attaching must work without the session setting.
    node.query(
        "CREATE TABLE t_stored (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS default_compression_codec = 'ZXC'",
        settings=ENABLED,
    )
    node.query("DETACH TABLE t_stored")
    node.query("ATTACH TABLE t_stored", settings=DISABLED)
    node.query("DROP TABLE t_stored SYNC")


def test_session_opt_in_is_not_durable_without_default_profile(started_cluster):
    # With the default profile at `enable_zxc_codec = 0`, a session-level
    # opt-in must not accept the config-inherited experimental default: nothing is
    # written into the table metadata, so the table would become unloadable after
    # a restart, when the value is re-validated against the default profile.
    error = node_profile_disabled.query_and_get_error(
        "CREATE TABLE t_session (x UInt64) ENGINE = MergeTree ORDER BY x",
        settings=ENABLED,
    )
    assert "default profile" in error

    # An explicit non-experimental override is stored and therefore fine.
    node_profile_disabled.query(
        "CREATE TABLE t_session (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS default_compression_codec = 'LZ4'",
        settings=ENABLED,
    )

    # Resetting the stored setting would fall back to the non-durable config
    # default, so it is rejected too, even with the session opt-in.
    error = node_profile_disabled.query_and_get_error(
        "ALTER TABLE t_session RESET SETTING default_compression_codec",
        settings=ENABLED,
    )
    assert "default profile" in error

    # An explicit experimental codec is stored in the metadata (durable), so the
    # session opt-in is sufficient for it.
    node_profile_disabled.query(
        "ALTER TABLE t_session MODIFY SETTING default_compression_codec = 'ZXC'",
        settings=ENABLED,
    )
    node_profile_disabled.query("DETACH TABLE t_session")
    node_profile_disabled.query("ATTACH TABLE t_session")
    node_profile_disabled.query("DROP TABLE t_session SYNC")


def test_restart_with_config_default_allowed_in_default_profile(started_cluster):
    # With the config default opted in via the default profile, existing tables
    # (stored codec settings or not) survive a server restart.
    node.query(
        "CREATE TABLE t_restart (x UInt64) ENGINE = MergeTree ORDER BY x",
        settings=ENABLED,
    )
    node.query("INSERT INTO t_restart VALUES (1)", settings=ENABLED)

    node.restart_clickhouse()

    assert node.query("SELECT count() FROM t_restart") == "1\n"
    node.query("DROP TABLE t_restart SYNC")


def test_restart_with_system_profile_not_repeating_the_opt_in(started_cluster):
    # The table stores no codec setting, so on every load the value falls back to the config default
    # and is re-validated. The default profile allows it; `system_profile` does not, and checking the
    # load context against it would refuse the table on restart.
    node_system_profile_disabled.query(
        "CREATE TABLE t_system_profile (x UInt64) ENGINE = MergeTree ORDER BY x",
        settings=ENABLED,
    )
    node_system_profile_disabled.query(
        "INSERT INTO t_system_profile VALUES (1)", settings=ENABLED
    )

    node_system_profile_disabled.restart_clickhouse()

    assert node_system_profile_disabled.query("SELECT count() FROM t_system_profile") == "1\n"
    node_system_profile_disabled.query("DROP TABLE t_system_profile SYNC")
