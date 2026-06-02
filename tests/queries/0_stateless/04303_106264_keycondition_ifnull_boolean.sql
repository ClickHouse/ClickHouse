DROP TABLE IF EXISTS t_ifnull_keycond;

CREATE TABLE t_ifnull_keycond (team_id UInt64, k UInt8, s String)
ENGINE = MergeTree ORDER BY (team_id, k, s)
SETTINGS index_granularity = 8192;

INSERT INTO t_ifnull_keycond SELECT 1, number % 5, toString(number) FROM numbers(200000);
OPTIMIZE TABLE t_ifnull_keycond FINAL;

SET allow_key_condition_coalesce_rewrite = 1;

-- A primary-key range on `k` must be built for the ifNull/coalesce-wrapped predicate,
-- just like the bare comparison (1 = range present). Without the fix the wrapped forms
-- produce no `k` range and these return 0.
SELECT countIf(explain ILIKE '%k in [0, 0]%') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND k = 0);
SELECT countIf(explain ILIKE '%k in [0, 0]%') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND ifNull(equals(k, 0), 0));
SELECT countIf(explain ILIKE '%k in [0, 0]%') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND coalesce(equals(k, 0), 0));

-- A truthy fallback must NOT be rewritten: ifNull(X, 1) is not equivalent to X, so no `k` range (0).
SELECT countIf(explain ILIKE '%k in [0, 0]%') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND ifNull(equals(k, 0), 1));

-- Results must be identical to the bare comparison (40000 rows with k = 0).
SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND k = 0;
SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND ifNull(equals(k, 0), 0);
SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND coalesce(equals(k, 0), 0);

-- Inversion guardrail with a nullable key: NOT ifNull(k = 0, 0) keeps NULL rows,
-- whereas NOT (k = 0) drops them. Assert the rewrite does not change results.
DROP TABLE IF EXISTS t_ifnull_nullable;
CREATE TABLE t_ifnull_nullable (k Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ifnull_nullable VALUES (0)(1)(NULL);
SELECT count() FROM t_ifnull_nullable WHERE ifNull(equals(k, 0), 0);
SELECT count() FROM t_ifnull_nullable WHERE NOT ifNull(equals(k, 0), 0);

DROP TABLE t_ifnull_nullable;
DROP TABLE t_ifnull_keycond;
