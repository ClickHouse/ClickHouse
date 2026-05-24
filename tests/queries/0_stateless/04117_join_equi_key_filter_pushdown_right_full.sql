-- Regression test: equi-key `WHERE` predicates must be pushed as an index
-- condition on the left `MergeTree` input for `RIGHT JOIN`. Previously,
-- `RIGHT`/`FULL JOIN` with a predicate on the `USING` column read every
-- granule of the left side. `INNER`/`LEFT` already optimised correctly.
--
-- For `FULL JOIN` the filter is also pushed, but the post-join output column
-- is wrapped in `firstNonDefault(l.k, r.k)`, which `KeyCondition` cannot
-- currently reduce to `k = c`, so granule pruning does not kick in. The test
-- asserts correctness for `FULL JOIN` and the granule count only for `RIGHT`.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 'false';
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS mt;
CREATE TABLE mt (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO mt SELECT number FROM numbers(1000000);

SELECT 'RIGHT JOIN USING, equi-key WHERE, matched types: left MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT k FROM mt AS l RIGHT JOIN (SELECT toUInt64(1) AS k) AS r USING (k) WHERE k = 1
) WHERE explain ILIKE '%Condition: (k in [1, 1])%';

SELECT 'RIGHT JOIN USING, equi-key WHERE, UInt8/UInt64 mismatch: left MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT k FROM mt AS l RIGHT JOIN (SELECT 1 AS k) AS r USING (k) WHERE k = 1
) WHERE explain ILIKE '%Condition: (k in [1, 1])%';

SELECT 'RIGHT JOIN USING, equi-key WHERE: result';
SELECT k FROM mt AS l RIGHT JOIN (SELECT 1 AS k) AS r USING (k) WHERE k = 1 ORDER BY k;

SELECT 'RIGHT ALL JOIN ON, equi-key WHERE: left MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT l.k FROM mt AS l RIGHT JOIN (SELECT toUInt64(1) AS k) AS r ON l.k = r.k WHERE r.k = 1
) WHERE explain ILIKE '%Condition: (k in [1, 1])%';

SELECT 'RIGHT ANTI JOIN USING, equi-key WHERE: correctness preserved';
SELECT k FROM mt AS l RIGHT ANTI JOIN (SELECT toUInt64(0) AS k UNION ALL SELECT toUInt64(1000000000) AS k) AS r USING (k) WHERE k = 1000000000 ORDER BY k;

SELECT 'FULL JOIN USING, equi-key WHERE: result (matched row)';
SELECT k FROM mt AS l FULL JOIN (SELECT toUInt64(1) AS k) AS r USING (k) WHERE k = 1 ORDER BY k;

SELECT 'FULL JOIN USING, equi-key WHERE: result (right-only contributing row preserved)';
SELECT k FROM mt AS l FULL JOIN (SELECT toUInt64(1000000000) AS k) AS r USING (k) WHERE k = 1000000000 ORDER BY k;

SELECT 'FULL JOIN USING, equi-key WHERE: left-only rows still reachable via filter';
SELECT count() FROM (
    SELECT k FROM mt AS l FULL JOIN (SELECT toUInt64(1000000000) AS k) AS r USING (k) WHERE k = 7
);

DROP TABLE mt;
