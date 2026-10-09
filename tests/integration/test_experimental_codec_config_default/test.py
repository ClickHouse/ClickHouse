"""An experimental codec (`ZXC`) set as a `<merge_tree>` config default must not enter tables
of sessions that did not enable `allow_experimental_codecs`, neither on CREATE, nor on load
(short `ATTACH`), nor via `ALTER TABLE ... RESET SETTING` falling back to the config default.

The default profile has `allow_experimental_codecs = 1` (the legitimate way for an operator
to opt in to such a config default), and the tests run individual queries with the setting
disabled to exercise the gate.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/merge_tree.xml"],
    user_configs=["configs/allow_experimental_codecs.xml"],
    stay_alive=True,
)

DISABLED = {"allow_experimental_codecs": 0}
ENABLED = {"allow_experimental_codecs": 1}


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
