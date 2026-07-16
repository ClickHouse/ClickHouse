-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/100929
-- IN (SELECT ...) with Tuple(LowCardinality(...)) ORDER BY key must not crash.
-- The fix in `tryPrepareSetColumnsForIndex` (PR #100760) strips `LowCardinality`
-- from the key column type before `castColumnAccurateOrNull`. This test ensures
-- the subquery IN path is also covered.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_lc_in_subquery;

CREATE TABLE t_lc_in_subquery
(
    id UInt64,
    key_tuple Tuple(LowCardinality(UInt32), UInt32),
    payload UInt64
)
ENGINE = MergeTree
ORDER BY key_tuple
SETTINGS index_granularity = 1000, allow_nullable_key = 1;

INSERT INTO t_lc_in_subquery SELECT number, (number, number % 10), number FROM numbers(1000);

-- Literal IN with out-of-range value: should return 0, not crash
SELECT count() FROM t_lc_in_subquery WHERE key_tuple IN ((-1, 0));

-- Subquery IN with out-of-range value: should return 0, not crash
SELECT count() FROM t_lc_in_subquery WHERE key_tuple IN (SELECT (toInt64(-1), toUInt32(0)));

-- Subquery IN with matching value: (10, 0) matches row where number=10
SELECT count() FROM t_lc_in_subquery WHERE key_tuple IN (SELECT (toUInt32(10), toUInt32(0)));

-- Subquery IN with multiple rows including out-of-range: only (10, 0) matches
SELECT count() FROM t_lc_in_subquery WHERE key_tuple IN (SELECT * FROM VALUES('v Tuple(Int64, UInt32)', (-1, 0), (10, 0), (-999, 5)));

DROP TABLE t_lc_in_subquery;
