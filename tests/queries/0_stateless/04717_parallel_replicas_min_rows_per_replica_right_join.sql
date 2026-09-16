-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/111206
-- With `parallel_replicas_min_number_of_rows_per_replica` > 0 the initiator analyzes the leftmost
-- leaf to estimate the replica count and hands that scan to the local plan, but the local plan
-- parallelizes the RIGHT table for a `RIGHT JOIN`, so the left leaf's parts and columns were applied
-- to the wrong table (`NOT_FOUND_COLUMN_IN_BLOCK` / `THERE_IS_NO_COLUMN`).
-- Results must be identical to running without parallel replicas.

DROP TABLE IF EXISTS tl;
DROP TABLE IF EXISTS tr;

CREATE TABLE tl (k Int32, a Int32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE tr (k Int32, ver Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO tl SELECT number, number FROM numbers(1000);
INSERT INTO tr SELECT number, number FROM numbers(500);
-- Keys 1000..1019 have no match in `tl`, so the `RIGHT JOIN` must keep those rows with a default
-- left side. Without them the join produces no unmatched rows and `RIGHT JOIN` semantics, along
-- with the silently-wrong-result variant of the bug, would go unchecked.
INSERT INTO tr SELECT number, number FROM numbers(1000, 20);

-- `automatic_parallel_replicas_mode` is a separate feature, pinned to 0 only because the runner
-- randomizes it to 2 and a non-zero mode forces `enable_parallel_replicas` to 0, which would leave
-- the path under test unexercised.
SET enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1,
    automatic_parallel_replicas_mode = 0,
    parallel_replicas_min_number_of_rows_per_replica = 100;

-- Liveness oracle: the results below are also produced by plain execution, so assert that the
-- query really runs with parallel replicas. Prints 1; prints 0 if the path is ever bypassed.
SELECT count() > 0 FROM (
    EXPLAIN SELECT r.ver, (l.a + 2) FROM tl AS l RIGHT JOIN tr AS r USING (k) ORDER BY r.ver LIMIT 5
) WHERE explain ILIKE '%ParallelReplicas%';

-- Projecting a left column under `RIGHT JOIN` (issue's Error 10 case).
SELECT r.ver, (l.a + 2) FROM tl AS l RIGHT JOIN tr AS r USING (k) ORDER BY r.ver LIMIT 5;

-- Unmatched right rows: the left side must come back as defaults, not as wrong values.
SELECT r.k, r.ver, (l.a + 2) FROM tl AS l RIGHT JOIN tr AS r USING (k) ORDER BY r.k DESC LIMIT 5;

-- Aggregate of a right non-key column with a `WHERE` (issue's Error 8 case).
SELECT uniqExact(r.ver) FROM tl AS l RIGHT JOIN tr AS r ON l.k = r.k WHERE r.k != 5;

-- `RIGHT ANTI JOIN` (issue's third manifestation).
SELECT count() FROM (SELECT r.ver, (l.a + 2) FROM tl AS l RIGHT ANTI JOIN tr AS r USING (k));

-- Self `RIGHT JOIN`: both sides share the same storage; the analysis reuse must still be skipped
-- for the right-branch scan.
SELECT r.k, (l.a + 2) FROM tl AS l RIGHT JOIN tl AS r USING (k) ORDER BY r.k LIMIT 5;

-- `LEFT` / `INNER` must keep working (the fix must not disable analysis reuse for them).
SELECT l.k, (r.ver + 2) FROM tl AS l LEFT JOIN tr AS r USING (k) ORDER BY l.k LIMIT 5;
SELECT l.a, r.ver FROM tl AS l INNER JOIN tr AS r USING (k) ORDER BY l.a LIMIT 5;

DROP TABLE tl;
DROP TABLE tr;
