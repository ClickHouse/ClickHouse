-- toNullable() is reported as a strictly-monotonic wrapper, so KeyCondition must be able to
-- prune primary-key granules through toNullable(key) exactly as it does for the bare key.
-- This regression guards the monotonicity contract added in toNullable.cpp: without it,
-- WHERE toNullable(k) = ... reads all granules (no pruning).
-- The assertions compare the "Granules: selected/total" line for toNullable(k) against the
-- bare key, so they are stable under randomized index_granularity / merge-tree settings.

SET enable_analyzer = 1;
-- The assertions are about local KeyCondition granule pruning; parallel replicas would route the
-- read through a cluster and change the plan shape (and needs a cluster this single-node run lacks).
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_tonullable_mono;
CREATE TABLE t_tonullable_mono (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_tonullable_mono SELECT number FROM numbers(1000000);

-- Equality: toNullable(k) prunes to the same granules as the bare key, and that is fewer than all.
WITH
    (SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tonullable_mono WHERE k = 42)
        WHERE explain ILIKE '%Granules: %/%' LIMIT 1) AS bare,
    (SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tonullable_mono WHERE toNullable(k) = 42)
        WHERE explain ILIKE '%Granules: %/%' LIMIT 1) AS wrapped
SELECT
    'eq: wrapped prunes like bare', bare = wrapped,
    'eq: pruning happened', toInt64(extract(wrapped, 'Granules: (\d+)/')) < toInt64(extract(wrapped, '/(\d+)'));

-- Range: same invariant for toNullable(k) < 100.
WITH
    (SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tonullable_mono WHERE k < 100)
        WHERE explain ILIKE '%Granules: %/%' LIMIT 1) AS bare,
    (SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tonullable_mono WHERE toNullable(k) < 100)
        WHERE explain ILIKE '%Granules: %/%' LIMIT 1) AS wrapped
SELECT
    'range: wrapped prunes like bare', bare = wrapped,
    'range: pruning happened', toInt64(extract(wrapped, 'Granules: (\d+)/')) < toInt64(extract(wrapped, '/(\d+)'));

-- Results are unchanged by the wrapper.
SELECT count() FROM t_tonullable_mono WHERE k = 42;
SELECT count() FROM t_tonullable_mono WHERE toNullable(k) = 42;
SELECT count() FROM t_tonullable_mono WHERE toNullable(k) < 100;

DROP TABLE t_tonullable_mono;
