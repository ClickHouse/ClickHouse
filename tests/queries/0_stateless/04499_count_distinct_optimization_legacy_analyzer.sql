-- https://github.com/ClickHouse/ClickHouse/issues/103069
-- `count_distinct_optimization` must not change the
-- result of `countDistinct` / `uniqExact` on `Nullable` / `LowCardinality(Nullable)`
-- columns, which skip `NULL`. This test forces the legacy analyzer (`enable_analyzer = 0`):
-- the rewrite there is only applied by the analyzer's `CountDistinctPass`, so the legacy
-- path must fall back to the plain aggregate and keep the correct result. The companion
-- test 04259 covers the analyzer path (`enable_analyzer = 1`).

SET enable_analyzer = 0;
SET count_distinct_optimization = 1;

DROP TABLE IF EXISTS t_cd_legacy_null;
CREATE TABLE t_cd_legacy_null (x Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cd_legacy_null VALUES (NULL)(NULL)(NULL);

-- All NULL: distinct-non-NULL count is 0, not 1.
SELECT countDistinct(x) FROM t_cd_legacy_null;
SELECT uniqExact(x)     FROM t_cd_legacy_null;
SELECT countDistinct(x) FROM t_cd_legacy_null SETTINGS count_distinct_optimization = 0;

DROP TABLE t_cd_legacy_null;

DROP TABLE IF EXISTS t_cd_legacy_mix;
CREATE TABLE t_cd_legacy_mix (x Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cd_legacy_mix VALUES (1)(NULL)(2)(NULL)(1);

SELECT countDistinct(x) FROM t_cd_legacy_mix;
SELECT uniqExact(x)     FROM t_cd_legacy_mix;
SELECT countDistinct(x) FROM t_cd_legacy_mix SETTINGS count_distinct_optimization = 0;

DROP TABLE t_cd_legacy_mix;

DROP TABLE IF EXISTS t_cd_legacy_lc;
CREATE TABLE t_cd_legacy_lc (x LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cd_legacy_lc VALUES ('a')(NULL)('b')(NULL)('a');

SELECT countDistinct(x) FROM t_cd_legacy_lc;
SELECT uniqExact(x)     FROM t_cd_legacy_lc;
SELECT countDistinct(x) FROM t_cd_legacy_lc SETTINGS count_distinct_optimization = 0;

DROP TABLE t_cd_legacy_lc;

DROP TABLE IF EXISTS t_cd_legacy_nonnull;
CREATE TABLE t_cd_legacy_nonnull (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cd_legacy_nonnull VALUES (1)(1)(2)(3);

-- Non-nullable: the optimization does not change the result either.
SELECT countDistinct(x) FROM t_cd_legacy_nonnull;
SELECT uniqExact(x)     FROM t_cd_legacy_nonnull;
SELECT countDistinct(x) FROM t_cd_legacy_nonnull SETTINGS count_distinct_optimization = 0;

DROP TABLE t_cd_legacy_nonnull;
