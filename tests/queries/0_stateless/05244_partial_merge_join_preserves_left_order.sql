-- Tags: no-parallel-replicas
-- Assertions on reading do not hold with parallel replicas.

-- The order-preserving probe of `PartialMergeJoin`: when the query plan needs the left stream's
-- row order (read-in-order through the join), `MergeJoin` matches over a sorted copy of the key
-- columns and emits the output in the original row order, instead of re-sorting the left block by
-- the join key (issues #109216 and #110662).

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;
SET read_in_order_use_virtual_row = 1;
SET query_plan_join_swap_table = 0;
SET join_use_nulls = 0;
SET join_algorithm = 'partial_merge';
-- Small right blocks: equal-key runs cross the sorted right block boundaries.
SET partial_merge_join_rows_in_right_blocks = 16;
-- Small output chunks: the emission is split into many continuation chunks.
SET max_joined_block_size_rows = 7;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt32, j UInt32) ENGINE = MergeTree ORDER BY k;
-- `j` does not follow the order of `k`, so the probe has to work through a real permutation.
INSERT INTO l SELECT number, (number * 37) % 150 FROM numbers(1000);

CREATE TABLE r (j UInt32, v String) ENGINE = MergeTree ORDER BY tuple();
-- Every j of [0, 125) exactly twice; j of [125, 150) stays unmatched on the left side.
INSERT INTO r SELECT number % 125, concat('v', toString(number)) FROM numbers(250);

SELECT '-- The left read must be in order';

SELECT groupArray(trim(explain)) FROM (
    EXPLAIN PLAN actions = 1
    SELECT l.k, r.v FROM l LEFT JOIN r ON l.j = r.j
    ORDER BY l.k LIMIT 3
) WHERE explain LIKE '%ReadType%';

SELECT groupArray(trim(explain)) FROM (
    EXPLAIN PLAN actions = 1
    SELECT l.k, r.v FROM l INNER JOIN r ON l.j = r.j
    ORDER BY l.k LIMIT 3
) WHERE explain LIKE '%ReadType%';

SELECT '-- INNER ALL: fanout 2, in the left order';

SELECT count(), uniqExact(k), arraySort(groupArray(materialize(k))) = groupArray(materialize(k)) FROM (
    SELECT l.k AS k FROM l INNER JOIN r ON l.j = r.j
    ORDER BY l.k
    SETTINGS max_threads = 1
);

SELECT '-- LEFT ALL: matched rows have fanout 2, unmatched rows get defaults, in the left order';

SELECT count(), countIf(v = ''), arraySort(groupArray(materialize(k))) = groupArray(materialize(k)) FROM (
    SELECT l.k AS k, r.v AS v FROM l LEFT JOIN r ON l.j = r.j
    ORDER BY l.k
    SETTINGS max_threads = 1
);

SELECT '-- LEFT ANY: exactly one output row per left row, in the left order';

SELECT count(), countIf(v = ''), arraySort(groupArray(materialize(k))) = groupArray(materialize(k)) FROM (
    SELECT l.k AS k, r.v AS v FROM l LEFT ANY JOIN r ON l.j = r.j
    ORDER BY l.k
    SETTINGS max_threads = 1
);

SELECT '-- INNER ANY: one output row per matched key (hash join parity), in the left order';

SELECT count(), uniqExact(k), arraySort(groupArray(materialize(k))) = groupArray(materialize(k)) FROM (
    SELECT l.k AS k FROM l INNER ANY JOIN r ON l.j = r.j
    ORDER BY l.k
    SETTINGS max_threads = 1
);

SELECT '-- The results match the hash join';

SELECT
    (SELECT sum(cityHash64(k, j, v)) FROM (
        SELECT l.k AS k, l.j AS j, r.v AS v FROM l INNER JOIN r ON l.j = r.j
    ))
    =
    (SELECT sum(cityHash64(k, j, v)) FROM (
        SELECT l.k AS k, l.j AS j, r.v AS v FROM l INNER JOIN r ON l.j = r.j
        SETTINGS join_algorithm = 'hash'
    ));

SELECT
    (SELECT sum(cityHash64(k, j, v)) FROM (
        SELECT l.k AS k, l.j AS j, r.v AS v FROM l LEFT JOIN r ON l.j = r.j
    ))
    =
    (SELECT sum(cityHash64(k, j, v)) FROM (
        SELECT l.k AS k, l.j AS j, r.v AS v FROM l LEFT JOIN r ON l.j = r.j
        SETTINGS join_algorithm = 'hash'
    ));

SELECT '-- A one-sided ON condition (mask column) with the order-preserving probe';

SELECT count(), countIf(v = ''), arraySort(groupArray(materialize(k))) = groupArray(materialize(k)) FROM (
    SELECT l.k AS k, r.v AS v FROM l LEFT JOIN r ON l.j = r.j AND l.k >= 500
    ORDER BY l.k
    SETTINGS max_threads = 1
);

SELECT '-- The right side flushed to disk (forced by max_rows_in_join)';

SELECT count(), uniqExact(k), arraySort(groupArray(materialize(k))) = groupArray(materialize(k)) FROM (
    SELECT l.k AS k FROM l INNER JOIN r ON l.j = r.j
    ORDER BY l.k
    SETTINGS max_threads = 1, max_rows_in_join = 32
);

SELECT '-- Repeated-key left rows share one match list: the match index stays linear';

-- 50000 equal-key left rows x 1000 sorted right blocks: a per-row match index would allocate
-- 50M entries (~600 MB) before any output and trip the memory limit; the shared lists need
-- 1000 ranges. LIMIT 1 also pins that no full-output materialization happens before emission.
DROP TABLE IF EXISTS l_repeated;
DROP TABLE IF EXISTS r_repeated;
CREATE TABLE l_repeated (k UInt32, j UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO l_repeated SELECT number, 0 FROM numbers(50000);
CREATE TABLE r_repeated (j UInt32, v UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO r_repeated SELECT 0, number FROM numbers(16000);

-- `max_block_size` is pinned so that the whole left side is one probe block (the test runner
-- randomizes it, and small probe blocks would shrink a per-row index below the memory limit).
SELECT k, j, v FROM l_repeated INNER JOIN r_repeated ON l_repeated.j = r_repeated.j
ORDER BY k, v LIMIT 1
SETTINGS max_memory_usage = 300000000, max_threads = 1, max_block_size = 65536;

DROP TABLE l_repeated;
DROP TABLE r_repeated;

SELECT '-- Wide rows: the output chunks are capped by bytes, not only by rows';

-- 16384 left rows of ~4 KB payload with a fanout of 32. With `max_joined_block_size_rows = 0`
-- the row cap falls back to DEFAULT_BLOCK_SIZE, and a chunk of 65409 such rows (~270 MB) trips
-- the memory limit; the byte cap keeps the chunks at ~4 MiB. `cityHash64(payload, v)` needs both
-- sides, so the wide payload cannot be computed away before the join.
DROP TABLE IF EXISTS l_wide;
DROP TABLE IF EXISTS r_wide;
CREATE TABLE l_wide (k UInt32, j UInt32, payload String) ENGINE = MergeTree ORDER BY k;
INSERT INTO l_wide SELECT number, number % 64, repeat('x', 4096) FROM numbers(16384);
CREATE TABLE r_wide (j UInt32, v UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO r_wide SELECT number % 64, number FROM numbers(2048);

SELECT k, cityHash64(payload, v) FROM l_wide INNER JOIN r_wide ON l_wide.j = r_wide.j
ORDER BY k, v LIMIT 1
SETTINGS max_memory_usage = 300000000, max_threads = 1, max_block_size = 65536,
         max_joined_block_size_rows = 0, max_joined_block_size_bytes = 4194304;

DROP TABLE l_wide;
DROP TABLE r_wide;

SELECT '-- DISTINCT in order over the join (issue #109216)';

SELECT count() FROM (
    SELECT DISTINCT l.k FROM l INNER JOIN r ON l.j = r.j
    SETTINGS optimize_distinct_in_order = 1
);

SELECT '-- Aggregation in order over the join (issue #110662)';

SELECT max(u), min(u), count() FROM (
    SELECT intDiv(l.k, 10) AS g, uniqExact(g) AS u FROM l LEFT JOIN r ON l.j = r.j
    GROUP BY g
    SETTINGS optimize_aggregation_in_order = 1, max_threads = 1
);

DROP TABLE l;
DROP TABLE r;
