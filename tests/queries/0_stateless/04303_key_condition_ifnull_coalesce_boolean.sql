-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ

SET explain_query_plan_default = 'legacy';

-- { echo }

SET enable_analyzer = 1;
SET allow_key_condition_coalesce_rewrite = 1;

DROP TABLE IF EXISTS t_ifnull_keycond;
CREATE TABLE t_ifnull_keycond (team_id UInt64, k UInt8, s String)
ENGINE = MergeTree ORDER BY (team_id, k, s) SETTINGS index_granularity = 8192;
INSERT INTO t_ifnull_keycond SELECT 1, number % 5, toString(number) FROM numbers(200000);

-- Bare comparison: the primary key builds a `k in [0, 0]` range.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND k = 0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND k = 0 SETTINGS force_primary_key = 1;

-- `ifNull(X, 0)` wrapper: same `Condition` and result as the bare comparison.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND ifNull(equals(k, 0), 0)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND ifNull(equals(k, 0), 0) SETTINGS force_primary_key = 1;

-- `coalesce(X, 0)` wrapper: same as `ifNull`.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND coalesce(equals(k, 0), 0)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND coalesce(equals(k, 0), 0) SETTINGS force_primary_key = 1;

-- A truthy fallback is not equivalent to `X`, so `ifNull(X, 1)` must not be rewritten: no `k` range,
-- only the `team_id` range remains.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND ifNull(equals(k, 0), 1)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';

-- No inner-function whitelist: any indexable predicate works under the wrapper.
-- `transform_null_in` turns GLOBAL IN into `globalNullIn`, which `KeyCondition` indexes as a set.
SET transform_null_in = 1;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND ifNull(globalNullIn(k, (0)), 0)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND ifNull(globalNullIn(k, (0)), 0) SETTINGS force_primary_key = 1;

-- `has` is turned into a set as well; 80000 rows have `k` in {0, 3}.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND ifNull(has([0, 3], k), 0)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND ifNull(has([0, 3], k), 0) SETTINGS force_primary_key = 1;

-- Boolean context propagates through `or`, so a wrapper in an OR branch is unwrapped too: same range
-- and pruning as the bare `k = 4 OR k = 0`. 80000 rows have `k` in {0, 4}.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND (k = 4 OR ifNull(equals(k, 0), 0))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND (k = 4 OR ifNull(equals(k, 0), 0)) SETTINGS force_primary_key = 1;

-- The rewrite fires at even `NOT` nesting (net non-inverted), not only at zero: under two `NOT`s the
-- wrapper is still unwrapped and prunes exactly like the bare `team_id = 1 AND k = 0`.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_keycond WHERE NOT (team_id != 1 OR NOT ifNull(equals(k, 0), 0))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM t_ifnull_keycond WHERE NOT (team_id != 1 OR NOT ifNull(equals(k, 0), 0)) SETTINGS force_primary_key = 1;

DROP TABLE t_ifnull_keycond;

-- Inversion guardrail on an indexed nullable key (granularity 1, so each row is its own granule).
-- Non-inverted `ifNull(equals(k, 0), 0)` builds the `k` range and keeps `k = 0` (1 row). The inverted
-- `NOT ifNull(...)` is not equivalent to the bare comparison on the `NULL` row, so it must not be
-- rewritten: the condition stays `true`, the primary key is not used, and `k = 1` and `k = NULL` are
-- kept (2 rows).
DROP TABLE IF EXISTS t_ifnull_nullable;
CREATE TABLE t_ifnull_nullable (k Nullable(UInt8)) ENGINE = MergeTree ORDER BY k
SETTINGS allow_nullable_key = 1, index_granularity = 1;
INSERT INTO t_ifnull_nullable VALUES (0)(1)(NULL);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_nullable WHERE ifNull(equals(k, 0), 0)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM t_ifnull_nullable WHERE ifNull(equals(k, 0), 0) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_nullable WHERE NOT ifNull(equals(k, 0), 0)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM t_ifnull_nullable WHERE NOT ifNull(equals(k, 0), 0);
SELECT count() FROM t_ifnull_nullable WHERE NOT ifNull(equals(k, 0), 0) SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

DROP TABLE t_ifnull_nullable;

-- Value-context guard: as a value argument of another function the wrapper is a value, not a condition, so the
-- boolean rewrite must not fire. `equals(ifNull(equals(k, 0), 0), 0)` keeps `k = 1` and `k = NULL`
-- (2 rows); a wrongly-dropped wrapper could prune the `NULL` granule on the functional key and lose it.
DROP TABLE IF EXISTS t_ifnull_value_ctx;
CREATE TABLE t_ifnull_value_ctx (k Nullable(UInt8)) ENGINE = MergeTree ORDER BY equals(k, 0)
SETTINGS allow_nullable_key = 1, index_granularity = 1;
INSERT INTO t_ifnull_value_ctx VALUES (0)(1)(NULL);

SELECT count() FROM t_ifnull_value_ctx WHERE equals(ifNull(equals(k, 0), 0), 0);

DROP TABLE t_ifnull_value_ctx;

-- Skip indexes build their own RPN from the canonicalized predicate, so they must prune through the
-- wrapper exactly like the bare comparison. `force_data_skipping_indices` asserts the index is used;
-- a truthy fallback `ifNull(X, 1)` is not rewritten and must not be used.
SET enable_full_text_index = 1;

DROP TABLE IF EXISTS t_ifnull_bf;
CREATE TABLE t_ifnull_bf (x String, INDEX idx x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_ifnull_bf SELECT number FROM numbers(1000);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM t_ifnull_bf WHERE x = '100') WHERE explain LIKE '%Granules%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM t_ifnull_bf WHERE ifNull(equals(x, '100'), 0)) WHERE explain LIKE '%Granules%';
SELECT count() FROM t_ifnull_bf WHERE ifNull(equals(x, '100'), 0) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_ifnull_bf WHERE coalesce(equals(x, '100'), 0) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_ifnull_bf WHERE ifNull(equals(x, '100'), 1) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE t_ifnull_bf;

DROP TABLE IF EXISTS t_ifnull_tb;
CREATE TABLE t_ifnull_tb (x String, INDEX idx x TYPE tokenbf_v1(16000, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_ifnull_tb SELECT number FROM numbers(1000);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM t_ifnull_tb WHERE ifNull(equals(x, '100'), 0)) WHERE explain LIKE '%Granules%';
SELECT count() FROM t_ifnull_tb WHERE ifNull(equals(x, '100'), 0) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_ifnull_tb WHERE coalesce(equals(x, '100'), 0) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_ifnull_tb;

DROP TABLE IF EXISTS t_ifnull_tx;
CREATE TABLE t_ifnull_tx (x String, INDEX idx x TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_ifnull_tx SELECT number FROM numbers(1000);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM t_ifnull_tx WHERE ifNull(equals(x, '100'), 0)) WHERE explain LIKE '%Granules%';
SELECT count() FROM t_ifnull_tx WHERE ifNull(equals(x, '100'), 0) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_ifnull_tx WHERE coalesce(equals(x, '100'), 0) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_ifnull_tx;

-- LIKE affix: `optimize_rewrite_like_perfect_affix` rewrites `s LIKE 'foo%'` to `startsWith(s, 'foo')`
-- before index analysis, so the wrapped form must build the same primary-key range as the bare one.
DROP TABLE IF EXISTS t_ifnull_affix;
CREATE TABLE t_ifnull_affix (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 8192;
INSERT INTO t_ifnull_affix SELECT toString(number) FROM numbers(100000);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_affix WHERE s LIKE 'foo%') WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_affix WHERE ifNull(like(s, 'foo%'), 0)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM t_ifnull_affix WHERE ifNull(like(s, 'foo%'), 0) SETTINGS force_primary_key = 1;

DROP TABLE t_ifnull_affix;
