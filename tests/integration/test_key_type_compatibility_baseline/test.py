import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Baseline (global default profile) pins compatibility = 24.1, where
# geo_distance_returns_float64_on_float64_arguments defaults to 0 (geoDistance over Float64 -> Float32)
# and function_date_trunc_return_type_behavior defaults to 1 (dateTrunc over DateTime64/Date32 -> Date/DateTime).
node_compat = cluster.add_instance(
    "node_compat", user_configs=["configs/compatibility.xml"], stay_alive=True
)
# Default profile: baseline is the current built-in defaults.
node_default = cluster.add_instance("node_default", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_geo_key_type_follows_compatibility_baseline_across_restart(started_cluster):
    # Under compatibility = 24.1 the geoDistance key column is Float32. The parts serialize primary.idx
    # with that type. A metadata recompute (metadata-only ALTER, restart) must keep the key type at the
    # server baseline (Float32) instead of the current built-in default (Float64); otherwise primary.idx
    # is deserialized with the wrong width and the read fails with CANNOT_READ_ALL_DATA.
    node_compat.query("DROP TABLE IF EXISTS tg SYNC")
    node_compat.query(
        "CREATE TABLE tg (a Float64, b Float64, c Float64, d Float64) "
        "ENGINE = MergeTree() ORDER BY geoDistance(a, b, c, d)"
    )
    node_compat.query("INSERT INTO tg VALUES (55.0, 37.0, 55.1, 37.1), (10, 20, 10.1, 20.1), (0, 0, 1, 1)")

    # Metadata-only ALTER: recompute the key type in a live query context.
    node_compat.query("ALTER TABLE tg MODIFY COMMENT 'touch'")
    node_compat.query("INSERT INTO tg VALUES (1, 2, 3, 4)")

    # Restart: reload the table from persisted metadata (uses the global/default-profile context).
    node_compat.restart_clickhouse()

    # Reading through primary.idx (force_primary_key) is what breaks when the recomputed key type
    # (Float64) diverges from the on-disk Float32 index.
    assert (
        node_compat.query(
            "SELECT count() FROM tg WHERE geoDistance(a, b, c, d) BETWEEN 0 AND 1e9 SETTINGS force_primary_key = 1"
        )
        == "4\n"
    )
    assert node_compat.query("SELECT count() FROM tg") == "4\n"
    node_compat.query("OPTIMIZE TABLE tg FINAL")
    assert node_compat.query("SELECT count() FROM tg") == "4\n"
    node_compat.query("DROP TABLE tg SYNC")


def test_date_trunc_key_type_follows_compatibility_baseline_across_restart(started_cluster):
    # Under compatibility = 24.1 function_date_trunc_return_type_behavior = 1, so dateTrunc over a
    # DateTime64 key resolves to the canonical DateTime (not the extended DateTime64). Same reload /
    # primary.idx-read stability requirement as the geo case.
    node_compat.query("DROP TABLE IF EXISTS td SYNC")
    node_compat.query(
        "CREATE TABLE td (ts DateTime64(3)) ENGINE = MergeTree() ORDER BY dateTrunc('hour', ts)"
    )
    node_compat.query("INSERT INTO td VALUES ('2020-01-01 00:00:00'), ('2021-06-15 12:30:00')")
    node_compat.query("ALTER TABLE td MODIFY COMMENT 'touch'")
    node_compat.query("INSERT INTO td VALUES ('2019-01-01 00:00:00')")
    node_compat.restart_clickhouse()
    assert (
        node_compat.query(
            "SELECT count() FROM td WHERE dateTrunc('hour', ts) >= '2000-01-01' SETTINGS force_primary_key = 1"
        )
        == "3\n"
    )
    node_compat.query("DROP TABLE td SYNC")


def test_transient_session_override_still_neutralized_on_default_profile(started_cluster):
    # Regression guard for #109181 on a server whose baseline is the built-in defaults: a per-query
    # SET of a type-affecting setting must NOT poison the persisted key type (it stays at the baseline),
    # so a following write with the default does not abort with a Bad cast.
    node_default.query("DROP TABLE IF EXISTS tg2 SYNC")
    node_default.query(
        "CREATE TABLE tg2 (a Float64, b Float64, c Float64, d Float64) "
        "ENGINE = MergeTree() ORDER BY geoDistance(a, b, c, d)"
    )
    # Transient override on a metadata-only ALTER: must be ignored for the key type.
    node_default.query(
        "ALTER TABLE tg2 MODIFY COMMENT 'x' SETTINGS geo_distance_returns_float64_on_float64_arguments = 0"
    )
    node_default.query("INSERT INTO tg2 VALUES (55, 37, 55.1, 37.1)")
    assert node_default.query("SELECT count() FROM tg2") == "1\n"
    # Baseline is the built-in default (Float64), independent of the transient session override.
    assert (
        node_default.query(
            "SELECT toTypeName(geoDistance(a, b, c, d)) FROM tg2 LIMIT 1 "
            "SETTINGS geo_distance_returns_float64_on_float64_arguments = 1"
        )
        == "Float64\n"
    )
    node_default.query(
        "SELECT count() FROM tg2 WHERE geoDistance(a, b, c, d) BETWEEN 0 AND 1e9 SETTINGS force_primary_key = 1"
    )
    node_default.query("DROP TABLE tg2 SYNC")
