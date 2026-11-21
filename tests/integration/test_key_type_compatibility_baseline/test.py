import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Baseline (global default profile) pins compatibility = 24.1, where
# geo_distance_returns_float64_on_float64_arguments defaults to 0 (geoDistance over Float64 -> Float32),
# function_date_trunc_return_type_behavior defaults to 1 (dateTrunc over DateTime64/Date32 -> Date/DateTime)
# and use_variant_as_common_type defaults to 0 (if/multiIf over branches with no lossless common type
# raise NO_COMMON_TYPE instead of resolving to a Variant).
node_compat = cluster.add_instance(
    "node_compat", user_configs=["configs/compatibility.xml"], stay_alive=True
)
# Default profile: baseline is the current built-in defaults.
node_default = cluster.add_instance("node_default", stay_alive=True)

# What each case can detect. createKeyExpressionContext resolves the pinned values from the server's
# global settings (so a non-default `compatibility` is honoured) rather than from the built-in literals.
# Only the two use_variant_as_common_type cases below detect a regression that swapped that baseline read
# for the built-in literals: their divergence is a different error code at DDL time. The geo and dateTrunc
# cases cannot, because a binary reading the literals is self-consistent - it would resolve the same
# (wrong) type at CREATE and at every recompute, so the parts are written to match and nothing diverges.
# Those cases instead pin the other half of the contract, that a transient session override is ignored.


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

    # Metadata-only ALTER carrying the opposite transient override: without the pin this recomputes the
    # key as Float64 while the parts on disk hold Float32, and the next write aborts with "Bad cast from
    # ColumnVector<float> to ColumnVector<double>". The pin has to resolve the baseline value (Float32),
    # not the session one.
    node_compat.query(
        "ALTER TABLE tg MODIFY COMMENT 'touch' "
        "SETTINGS geo_distance_returns_float64_on_float64_arguments = 1"
    )
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
    # Opposite transient override, as in the geo case: without the pin the recomputed key becomes the
    # extended DateTime64 while the parts hold DateTime, and the next write aborts with "Bad cast from
    # ColumnVector<unsigned int> to ColumnDecimal<DateTime64>".
    node_compat.query(
        "ALTER TABLE td MODIFY COMMENT 'touch' SETTINGS function_date_trunc_return_type_behavior = 0"
    )
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
    # Assert the row actually comes back: reading through primary.idx has to agree with the persisted
    # key type, and a wrong count would otherwise pass unnoticed.
    assert (
        node_default.query(
            "SELECT count() FROM tg2 WHERE geoDistance(a, b, c, d) BETWEEN 0 AND 1e9 SETTINGS force_primary_key = 1"
        )
        == "1\n"
    )
    node_default.query("DROP TABLE tg2 SYNC")


def test_variant_key_type_follows_compatibility_baseline(started_cluster):
    # use_variant_as_common_type decides how if/multiIf resolve branches with no lossless common type,
    # and it flips with the profile (built-in default 1, but 0 up to 26.1). It must be pinned to the
    # server baseline, not to the built-in literal: under compatibility = 24.1 the baseline resolves
    # if(c, Decimal64, Float64) as NO_COMMON_TYPE, while the built-in literal resolves it as
    # Variant(Decimal(18, 3), Float64) and then rejects the key for a different reason. The two error
    # codes differ, so this asserts the helper really reads the baseline (a regression falling back to
    # the built-in literals reports DATA_TYPE_CANNOT_BE_USED_IN_KEY here instead).
    node_compat.query("DROP TABLE IF EXISTS tv SYNC")
    assert "NO_COMMON_TYPE" in node_compat.query_and_get_error(
        "CREATE TABLE tv (c UInt8, dec Decimal64(3), f64 Float64) "
        "ENGINE = MergeTree() ORDER BY if(c, dec, f64)"
    )
    # Same assertion at a key-recomputation boundary carrying a transient override, which is where the
    # session settings could actually leak in. The key cannot be created at all under this baseline, so
    # the boundary has to be an ALTER MODIFY ORDER BY on a table that starts with a resolvable key.
    node_compat.query(
        "CREATE TABLE tv (c UInt8, dec Decimal64(3), f64 Float64) ENGINE = MergeTree() ORDER BY c"
    )
    assert "NO_COMMON_TYPE" in node_compat.query_and_get_error(
        "ALTER TABLE tv MODIFY ORDER BY (c, if(c, dec, f64)) "
        "SETTINGS use_variant_as_common_type = 1"
    )
    node_compat.query("INSERT INTO tv VALUES (1, 1.5, 2.5)")
    assert node_compat.query("SELECT count() FROM tv") == "1\n"
    node_compat.query("DROP TABLE tv SYNC")


def test_variant_index_type_follows_compatibility_baseline(started_cluster):
    # Same setting, but through the skip-index seam and with a transient per-query override, which is
    # the shape that actually poisons metadata on a compatibility-profile server. Only the `set` index
    # accepts a Variant column (minmax/bloom_filter reject it up front), so `set` is required here.
    #
    # Without the pin, ADD INDEX under use_variant_as_common_type = 1 records a Variant index type that
    # the baseline cannot resolve at all, and the table then fails to ATTACH on every subsequent load
    # (NO_COMMON_TYPE while loading the metadata) rather than only failing the write. With the pin the
    # index expression is resolved under the baseline, so the ALTER is rejected up front and the table
    # stays loadable.
    node_compat.query("DROP TABLE IF EXISTS tvi SYNC")
    node_compat.query(
        "CREATE TABLE tvi (c UInt8, dec Decimal64(3), f64 Float64) ENGINE = MergeTree() ORDER BY tuple()"
    )
    assert "NO_COMMON_TYPE" in node_compat.query_and_get_error(
        "ALTER TABLE tvi ADD INDEX idx if(c, dec, f64) TYPE set(0) GRANULARITY 1 "
        "SETTINGS use_variant_as_common_type = 1"
    )
    # No index was recorded, and the table survives a reload with the baseline profile.
    assert (
        node_compat.query(
            "SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tvi'"
        )
        == "0\n"
    )
    node_compat.restart_clickhouse()
    node_compat.query("INSERT INTO tvi VALUES (1, 1.5, 2.5)")
    assert node_compat.query("SELECT count() FROM tvi") == "1\n"
    node_compat.query("DROP TABLE tvi SYNC")
