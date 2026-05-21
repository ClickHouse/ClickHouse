-- Regression test: when a PREWHERE expression contains `IN (subquery)`, the
-- subquery's Set used to be eagerly built non-ordered in ReadFromMergeTree::applyFilters.
-- That preempted later buildOrderedSetInplace calls coming from skip-index /
-- KeyCondition analysis (which returned nullptr for an already-built non-ordered Set),
-- silently making `IN (subquery)` invisible to PK and skip-index pruning and breaking
-- `force_data_skipping_indices`.

DROP TABLE IF EXISTS t_prewhere_in_subq;

CREATE TABLE t_prewhere_in_subq
(
    a UInt64,
    b String,
    p UInt32,
    INDEX pk_set (a, b) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY (a, b);

INSERT INTO t_prewhere_in_subq SELECT number, toString(number), number % 10 FROM numbers(1000);

-- 1. Inline subquery in PREWHERE -- must use both PK and skip index.
SELECT count() FROM t_prewhere_in_subq
PREWHERE p = 5 AND (a, b) IN (SELECT 5 AS x, '5' AS y)
SETTINGS force_data_skipping_indices = 'pk_set';

-- 2. PREWHERE without partition filter -- same requirement.
SELECT count() FROM t_prewhere_in_subq
PREWHERE (a, b) IN (SELECT 7 AS x, '7' AS y)
SETTINGS force_data_skipping_indices = 'pk_set';

-- 3. MATERIALIZED CTE -- same requirement.
WITH d AS MATERIALIZED (SELECT 5 AS x, '5' AS y)
SELECT count() FROM t_prewhere_in_subq
PREWHERE p = 5 AND (a, b) IN (SELECT x, y FROM d)
SETTINGS force_data_skipping_indices = 'pk_set';

-- 4. Multi-row subquery.
SELECT count() FROM t_prewhere_in_subq
PREWHERE p = 5 AND (a, b) IN (SELECT toUInt64(5), '5' UNION ALL SELECT toUInt64(15), '15')
SETTINGS force_data_skipping_indices = 'pk_set';

-- 5. Empty subquery result is fine.
SELECT count() FROM t_prewhere_in_subq
PREWHERE p = 5 AND (a, b) IN (SELECT 5 AS x, '5' AS y WHERE 0)
SETTINGS force_data_skipping_indices = 'pk_set';

DROP TABLE t_prewhere_in_subq;
