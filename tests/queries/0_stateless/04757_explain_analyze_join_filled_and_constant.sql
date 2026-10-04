-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN ANALYZE rejects distributed plans (NOT_IMPLEMENTED).

-- Covers the three join implementations that `04603_explain_analyze_join_matched_rows` cannot reach,
-- because none of them is selectable through `join_algorithm` over two plain MergeTree tables:
-- `ConstantJoin` (a `CROSS` join or a constant `ON` condition), the `Join` table engine and
-- `DirectKeyValueJoin` (a dictionary looked up directly). The last two run under `FilledJoinStep`
-- rather than `JoinStep`.
--
-- The data is the one of 04603 (439 matching left rows, 521 matching right ones), so the numbers are
-- comparable across tests. A one-sided report still has `match rate` and `fanout` when the absent side
-- is not a preserved one: this test pins the groups, not only their values.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET join_use_nulls = 0;

DROP TABLE IF EXISTS left_side;
DROP TABLE IF EXISTS right_side;
DROP TABLE IF EXISTS right_join_engine;
DROP DICTIONARY IF EXISTS right_dictionary;

CREATE TABLE left_side (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE right_side (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO left_side SELECT number % 100, number FROM numbers(439);
INSERT INTO left_side SELECT 1000 + number, number FROM numbers(561);
INSERT INTO right_side SELECT number % 100, number FROM numbers(521);
INSERT INTO right_side SELECT 2000 + number, number FROM numbers(479);

SELECT 'ground truth: rows, matched';
SELECT count(), countIf(k IN (SELECT k FROM right_side)) FROM left_side;
SELECT count(), countIf(k IN (SELECT k FROM left_side)) FROM right_side;

-- `ConstantJoin` takes over whenever the `ON` section carries no key equality, so every left row is
-- paired with every right row. It counts the rows flowing through each side but never which of them
-- matched, since with a constant predicate the question has no per-row answer.
SELECT 'ConstantJoin: CROSS';
SELECT
    countIf(explain LIKE '%Left: rows%') AS left_group,
    countIf(explain LIKE '%Right: rows%') AS right_group,
    countIf(explain LIKE '%Buffer:%') AS buffer_group,
    countIf(explain LIKE '%Spill:%') AS spill_group,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM left_side CROSS JOIN (SELECT * FROM right_side LIMIT 3) AS r);

SELECT 'ConstantJoin: constant ON';
SELECT
    countIf(explain LIKE '%Left: rows%') AS left_group,
    countIf(explain LIKE '%Right: rows%') AS right_group,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM left_side ALL INNER JOIN (SELECT * FROM right_side LIMIT 3) AS r ON 1 = 1);

-- The `Join` engine is built during `INSERT` and skips the build pipeline entirely, so its clone
-- gets its statistics in `reuseJoinedData` instead of `onBuildPhaseFinish`. The counts must match
-- what the same kind and strictness report over two plain tables in 04603.
SELECT 'Join engine ALL INNER';
CREATE TABLE right_join_engine (k UInt64, v UInt64) ENGINE = Join(ALL, INNER, k);
INSERT INTO right_join_engine SELECT number % 100, number FROM numbers(521);
INSERT INTO right_join_engine SELECT 2000 + number, number FROM numbers(479);
SELECT
    countIf(explain LIKE '%Left: rows%') AS left_group,
    countIf(explain LIKE '%Right: rows%') AS right_group,
    countIf(explain LIKE '%Hash table:%') AS hash_table_group,
    countIf(explain LIKE '%match rate%') AS match_rate_group,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM left_side ALL INNER JOIN right_join_engine ON left_side.k = right_join_engine.k);

-- Without `matches = 1` the left side is still exact, because `ALL` derives it from the default-row
-- markers the probe writes anyway, while the right side needs the per-row flags and drops out.
SELECT 'Join engine ALL INNER, no matches';
SELECT
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE SELECT count() FROM left_side ALL INNER JOIN right_join_engine ON left_side.k = right_join_engine.k);

-- An `ANY` engine keeps one row per key, so neither side is derivable, exactly as for `ANY LEFT`
-- over two plain tables.
SELECT 'Join engine ANY LEFT';
DROP TABLE IF EXISTS right_join_engine_any;
CREATE TABLE right_join_engine_any (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO right_join_engine_any SELECT number % 100, number FROM numbers(521);
INSERT INTO right_join_engine_any SELECT 2000 + number, number FROM numbers(479);
SELECT
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM left_side ANY LEFT JOIN right_join_engine_any ON left_side.k = right_join_engine_any.k);

-- A dictionary is looked up per left row, so the left side is exact, but the right side is a
-- key-value store that is never materialized into rows and therefore has no group of its own. The
-- `Left:` line still carries the whole shape, since `direct` accepts only `INNER` and `LEFT` kinds.
SELECT 'Direct ALL INNER';
CREATE DICTIONARY right_dictionary (k UInt64, v UInt64) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'right_side')) LAYOUT(HASHED()) LIFETIME(0);
SELECT
    countIf(explain LIKE '%Left: rows%') AS left_group,
    countIf(explain LIKE '%Right: rows%') AS right_group,
    countIf(explain LIKE '%match rate%') AS match_rate_group,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'match rate ([0-9.]+)%'), explain LIKE '%Left: rows%') AS match_rate_left,
    maxIf(extract(explain, 'fanout ([0-9.]+)'), explain LIKE '%Left: rows%') AS fanout_left
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM left_side ALL INNER JOIN right_dictionary ON left_side.k = right_dictionary.k SETTINGS join_algorithm = 'direct');

-- `ALL LEFT` keeps the 561 left rows that found no dictionary entry, so here the matched output rows
-- are the ones left after subtracting them - the other branch of the same derivation.
SELECT 'Direct ALL LEFT';
SELECT
    countIf(explain LIKE '%Left: rows%') AS left_group,
    countIf(explain LIKE '%Right: rows%') AS right_group,
    countIf(explain LIKE '%match rate%') AS match_rate_group,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'match rate ([0-9.]+)%'), explain LIKE '%Left: rows%') AS match_rate_left,
    maxIf(extract(explain, 'fanout ([0-9.]+)'), explain LIKE '%Left: rows%') AS fanout_left
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM left_side ALL LEFT JOIN right_dictionary ON left_side.k = right_dictionary.k SETTINGS join_algorithm = 'direct');

DROP DICTIONARY right_dictionary;
DROP TABLE right_join_engine_any;
DROP TABLE right_join_engine;
DROP TABLE right_side;
DROP TABLE left_side;
