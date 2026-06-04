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

-- transform_null_in makes GLOBAL IN / GLOBAL NOT IN emit globalNullIn / globalNotNullIn, which
-- KeyCondition indexes like the other IN variants. The wrapped form must build the key range too
-- (printed as "k in 1-element set").
SELECT countIf(explain ILIKE '%k in 1-element set%') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND ifNull(globalNullIn(k, (0)), 0));
SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND ifNull(globalNullIn(k, (0)), 0);

-- The rewrite has no inner-function whitelist (truthiness of ifNull(X, 0) equals truthiness of X for
-- any X), so other indexable predicates work too, e.g. `has`, which KeyCondition turns into a set
-- (printed as "k in 2-element set"). 80000 rows have k in {0, 3}.
SELECT countIf(explain ILIKE '%k in 2-element set%') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND has([0, 3], k));
SELECT countIf(explain ILIKE '%k in 2-element set%') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND ifNull(has([0, 3], k), 0));
SELECT count() FROM t_ifnull_keycond WHERE team_id = 1 AND ifNull(has([0, 3], k), 0);

-- Inversion guardrail with an INDEXED nullable key (granularity 1 so each row is its own granule).
-- The non-inverted wrapper must build a `k` range; the inverted `NOT ifNull(...)` must not, because
-- under NOT the wrapper is not equivalent to the bare comparison on NULL rows. A regressed
-- need_inversion guard would prune the NULL granule and drop a row that should pass.
DROP TABLE IF EXISTS t_ifnull_nullable;
CREATE TABLE t_ifnull_nullable (k Nullable(UInt8)) ENGINE = MergeTree ORDER BY k
SETTINGS allow_nullable_key = 1, index_granularity = 1;
INSERT INTO t_ifnull_nullable VALUES (0)(1)(NULL);

-- Non-inverted builds the range (1); inverted does not (0).
SELECT countIf(explain ILIKE '%k in [0, 0]%') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_nullable WHERE ifNull(equals(k, 0), 0));
SELECT countIf(explain ILIKE '%k in [0, 0]%') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_nullable WHERE NOT ifNull(equals(k, 0), 0));

-- Results unchanged: ifNull keeps k = 0 (1 row); NOT keeps k = 1 and k = NULL (2 rows).
SELECT count() FROM t_ifnull_nullable WHERE ifNull(equals(k, 0), 0);
SELECT count() FROM t_ifnull_nullable WHERE NOT ifNull(equals(k, 0), 0);

DROP TABLE t_ifnull_nullable;
DROP TABLE t_ifnull_keycond;

-- Value-context guard: when the wrapper is a VALUE argument of another function (not truth-tested),
-- the boolean rewrite must NOT fire. equals(ifNull(equals(k, 0), 0), 0) keeps k = 1 and k = NULL
-- (2 rows). If the rewrite wrongly dropped the ifNull it would become equals(equals(k, 0), 0), and
-- KeyCondition could prune the NULL granule on the functional key equals(k, 0) and return 1. The
-- count is the robust check: a value-preserving rewrite may still prune the k = 0 granule, but the
-- NULL row must survive.
DROP TABLE IF EXISTS t_ifnull_value_ctx;
CREATE TABLE t_ifnull_value_ctx (k Nullable(UInt8)) ENGINE = MergeTree ORDER BY equals(k, 0)
SETTINGS allow_nullable_key = 1, index_granularity = 1;
INSERT INTO t_ifnull_value_ctx VALUES (0)(1)(NULL);

SELECT count() FROM t_ifnull_value_ctx WHERE equals(ifNull(equals(k, 0), 0), 0);

DROP TABLE t_ifnull_value_ctx;

-- The rewrite is applied to the shared canonicalized predicate that every secondary
-- index consumes (ReadFromMergeTree::buildIndexes feeds `filter_dag.predicate` to each
-- `createIndexCondition`), so skip indexes that build their own RPN from the predicate
-- must prune through the ifNull/coalesce wrapper exactly as the bare comparison does.
-- 1 = the skip index pruned granules (used < total). A truthy fallback ifNull(X, 1) is
-- not rewritten, so it must not prune (0).
SET enable_analyzer = 1;
SET enable_full_text_index = 1;

DROP TABLE IF EXISTS t_ifnull_bf;
CREATE TABLE t_ifnull_bf (x String, INDEX idx x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_ifnull_bf SELECT number FROM numbers(1000);

SELECT countIf(explain LIKE '%Granules:%' AND toUInt32OrZero(extract(explain, 'Granules: ([0-9]+)')) < toUInt32OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)'))) FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_ifnull_bf WHERE x = '100');
SELECT countIf(explain LIKE '%Granules:%' AND toUInt32OrZero(extract(explain, 'Granules: ([0-9]+)')) < toUInt32OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)'))) FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_ifnull_bf WHERE ifNull(equals(x, '100'), 0));
SELECT countIf(explain LIKE '%Granules:%' AND toUInt32OrZero(extract(explain, 'Granules: ([0-9]+)')) < toUInt32OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)'))) FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_ifnull_bf WHERE coalesce(equals(x, '100'), 0));
SELECT countIf(explain LIKE '%Granules:%' AND toUInt32OrZero(extract(explain, 'Granules: ([0-9]+)')) < toUInt32OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)'))) FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_ifnull_bf WHERE ifNull(equals(x, '100'), 1));
DROP TABLE t_ifnull_bf;

DROP TABLE IF EXISTS t_ifnull_tb;
CREATE TABLE t_ifnull_tb (x String, INDEX idx x TYPE tokenbf_v1(16000, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_ifnull_tb SELECT number FROM numbers(1000);

SELECT countIf(explain LIKE '%Granules:%' AND toUInt32OrZero(extract(explain, 'Granules: ([0-9]+)')) < toUInt32OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)'))) FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_ifnull_tb WHERE x = '100');
SELECT countIf(explain LIKE '%Granules:%' AND toUInt32OrZero(extract(explain, 'Granules: ([0-9]+)')) < toUInt32OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)'))) FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_ifnull_tb WHERE ifNull(equals(x, '100'), 0));
SELECT countIf(explain LIKE '%Granules:%' AND toUInt32OrZero(extract(explain, 'Granules: ([0-9]+)')) < toUInt32OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)'))) FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_ifnull_tb WHERE coalesce(equals(x, '100'), 0));
DROP TABLE t_ifnull_tb;

DROP TABLE IF EXISTS t_ifnull_tx;
CREATE TABLE t_ifnull_tx (x String, INDEX idx x TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_ifnull_tx SELECT number FROM numbers(1000);

SELECT countIf(explain LIKE '%Granules:%' AND toUInt32OrZero(extract(explain, 'Granules: ([0-9]+)')) < toUInt32OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)'))) FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_ifnull_tx WHERE x = '100');
SELECT countIf(explain LIKE '%Granules:%' AND toUInt32OrZero(extract(explain, 'Granules: ([0-9]+)')) < toUInt32OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)'))) FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_ifnull_tx WHERE ifNull(equals(x, '100'), 0));
SELECT countIf(explain LIKE '%Granules:%' AND toUInt32OrZero(extract(explain, 'Granules: ([0-9]+)')) < toUInt32OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)'))) FROM (
    EXPLAIN indexes = 1 SELECT * FROM t_ifnull_tx WHERE coalesce(equals(x, '100'), 0));
DROP TABLE t_ifnull_tx;

-- LIKE affix: with the analyzer, optimize_rewrite_like_perfect_affix (default on) rewrites
-- `s LIKE 'foo%'` to startsWith(s, 'foo') before key analysis, so the wrapped form must prune the
-- primary key like the bare predicate. 1 = pruned.
SET enable_analyzer = 1;
DROP TABLE IF EXISTS t_ifnull_affix;
CREATE TABLE t_ifnull_affix (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 8192;
INSERT INTO t_ifnull_affix SELECT toString(number) FROM numbers(100000);

SELECT countIf(explain LIKE '%Granules:%' AND toUInt32OrZero(extract(explain, 'Granules: ([0-9]+)')) < toUInt32OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)'))) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_affix WHERE s LIKE 'foo%');
SELECT countIf(explain LIKE '%Granules:%' AND toUInt32OrZero(extract(explain, 'Granules: ([0-9]+)')) < toUInt32OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)'))) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_ifnull_affix WHERE ifNull(like(s, 'foo%'), 0));
DROP TABLE t_ifnull_affix;
