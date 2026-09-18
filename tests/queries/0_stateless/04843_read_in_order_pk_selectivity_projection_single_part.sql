-- Tags: no-random-settings, no-random-merge-tree-settings
-- The projection exemption of the read-in-order PK-selectivity guard
-- (`read_in_order_max_primary_key_ratio`) is scoped to projection reads with more than one
-- selected part: the in-order read pool assigns each part to a single stream and never splits a
-- part, so once background merges collapse the projection to one part, the in-order read
-- degenerates to a single stream — exactly the parallelism loss the guard exists to avoid — while
-- the parallel-read-plus-sort fallback still splits mark ranges inside the part. Measurements
-- backing the one-part crossover are in the setting description and next to the check in
-- `requestReadingInOrder`. `04648_read_in_order_pk_selectivity_projection` covers the multi-part
-- exemption; this test covers the flip after a merge.

DROP TABLE IF EXISTS t_read_in_order_projection_single_part;

CREATE TABLE t_read_in_order_projection_single_part
(
    ts UInt64,
    path String,
    v UInt64,
    PROJECTION p_by_path
    (
        SELECT ts, path, v
        ORDER BY path
    )
)
ENGINE = MergeTree ORDER BY ts
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_read_in_order_projection_single_part;

INSERT INTO t_read_in_order_projection_single_part SELECT number, concat('dir', toString(number % 997), '/file.log'), number FROM numbers(0, 25000);
INSERT INTO t_read_in_order_projection_single_part SELECT number, concat('dir', toString(number % 997), '/file.log'), number FROM numbers(25000, 25000);

SET max_threads = 4;
SET enable_parallel_replicas = 0;

-- With two parts the projection read keeps its parallelism, so it is exempt from the guard:
-- read-in-order is kept and no sort is inserted, although the leading-wildcard `LIKE` gives the
-- projection's own primary key no pruning at all (selectivity ratio 1.0 > 0.5).
SELECT 'two_parts_exempt';
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT ts, path FROM t_read_in_order_projection_single_part
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS read_in_order_max_primary_key_ratio = 0.5, optimize_use_projections = 1
) WHERE explain LIKE '%PartialSortingTransform%';

-- Collapse the table (and with it the projection) to a single part, as background merges would.
SYSTEM START MERGES t_read_in_order_projection_single_part;
OPTIMIZE TABLE t_read_in_order_projection_single_part FINAL;

SELECT 'merged_to_single_part';
SELECT count() FROM system.parts WHERE database = currentDatabase()
    AND table = 't_read_in_order_projection_single_part' AND active AND rows > 0;

-- The projection is still used to satisfy the `ORDER BY`.
SELECT 'projection_used';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT ts, path FROM t_read_in_order_projection_single_part
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS read_in_order_max_primary_key_ratio = 0.5, optimize_use_projections = 1
) WHERE explain LIKE '%p_by_path%';

-- But the single-part projection read now obeys the guard: reading it in order would use a single
-- stream, so read-in-order is rejected and the parallel-read-plus-sort fallback appears.
SELECT 'single_part_guard_fires';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT ts, path FROM t_read_in_order_projection_single_part
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS read_in_order_max_primary_key_ratio = 0.5, optimize_use_projections = 1
) WHERE explain LIKE '%PartialSortingTransform%';

-- With the guard switched off (the default threshold 1.0), the single-part projection read keeps
-- read-in-order, which is what makes the case above a real guard hit.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT ts, path FROM t_read_in_order_projection_single_part
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS read_in_order_max_primary_key_ratio = 1., optimize_use_projections = 1
) WHERE explain LIKE '%PartialSortingTransform%';

-- Correctness: the fallback returns exactly the same ordered result as the in-order plan.
SELECT 'correctness';
SELECT count() FROM (
    SELECT ts, path FROM t_read_in_order_projection_single_part
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS read_in_order_max_primary_key_ratio = 0.5, optimize_use_projections = 1
);
SELECT
    (SELECT groupArray(path) FROM (
        SELECT path FROM t_read_in_order_projection_single_part
        WHERE path LIKE '%file.log'
        ORDER BY path
        SETTINGS read_in_order_max_primary_key_ratio = 0.5, optimize_use_projections = 1
    ))
    =
    (SELECT groupArray(path) FROM (
        SELECT path FROM t_read_in_order_projection_single_part
        WHERE path LIKE '%file.log'
        ORDER BY path
        SETTINGS read_in_order_max_primary_key_ratio = 1., optimize_use_projections = 1
    ));

DROP TABLE t_read_in_order_projection_single_part;
