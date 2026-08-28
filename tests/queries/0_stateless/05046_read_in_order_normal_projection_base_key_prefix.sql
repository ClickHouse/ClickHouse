-- A normal projection must not be preferred over a base-table read that already satisfies the
-- whole ORDER BY of an ORDER BY ... LIMIT query, even when the projection reads fewer marks.

SET optimize_read_in_order = 1;
SET optimize_use_projections = 1;
SET optimize_use_projection_filtering = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET read_in_order_use_virtual_row = 1;
SET query_plan_optimize_lazy_materialization = 0;
SET use_top_k_dynamic_filtering = 0;
SET use_skip_indexes_for_top_k = 0;
SET use_statistics_for_part_pruning = 0;
SET enable_parallel_replicas = 0;
SET parallel_replicas_for_non_replicated_merge_tree = 0;

DROP TABLE IF EXISTS t_bp;

-- index_granularity_bytes is pinned small so the wide base rows land ~10 rows per granule while the
-- projection's narrow rows land ~670, which is what makes the projection read fewer marks.
CREATE TABLE t_bp
(
    k1 String, k2 UInt32, k3 Bool, k4 UInt32, narrow UInt32,
    wide1 String, wide2 String, wide3 String,
    PROJECTION p_narrow (SELECT k1, k2, k3, k4, narrow ORDER BY k1, k2, k4)
)
ENGINE = MergeTree ORDER BY (k1, k2, k3, k4)
SETTINGS index_granularity = 8192, index_granularity_bytes = 16384;

INSERT INTO t_bp SELECT 'a', intDiv(number, 500), (number % 3) = 0, number, number,
    repeat('w', 500), repeat('x', 500), repeat('y', 500) FROM numbers(4000);

SELECT 'A1', count() FROM (EXPLAIN actions = 1 SELECT k1, k2, k3, k4, narrow FROM t_bp
    WHERE k4 < 3500 ORDER BY k1, k2, k3, k4 LIMIT 10) WHERE explain LIKE '%ReadFromMergeTree (%.t_bp)%';

SELECT 'A2', count() FROM (EXPLAIN actions = 1 SELECT k1, k2, k3, k4, narrow FROM t_bp
    WHERE k4 < 3500 ORDER BY k1, k2, k3, k4 LIMIT 10 SETTINGS optimize_use_projections = 0)
    WHERE explain LIKE '%ReadFromMergeTree (%.t_bp)%';

SELECT 'A3', count() FROM (EXPLAIN actions = 1 SELECT k1, k2, k3, k4, narrow FROM t_bp
    WHERE k4 < 3500 ORDER BY k1, k2, k3, k4 LIMIT 10 SETTINGS optimize_read_in_order = 0)
    WHERE explain LIKE '%ReadFromMergeTree (p_narrow)%';

SELECT 'A4', count() FROM (EXPLAIN actions = 1 SELECT k1, k2, k3, k4, narrow FROM t_bp
    WHERE k4 < 3500 ORDER BY k1, k2, k3, k4 LIMIT 10)
    WHERE explain LIKE '%Prefix sort description:%k1%k2%k3%k4%';

SELECT 'A6', count() FROM (EXPLAIN actions = 1 SELECT k1, k2, k3, k4, narrow FROM t_bp
    WHERE k4 < 3500 ORDER BY k1, k2, k3, k4) WHERE explain LIKE '%ReadFromMergeTree (p_narrow)%';

SELECT 'A7', count() FROM (EXPLAIN actions = 1 SELECT k1, k2, k3, k4, narrow FROM t_bp
    WHERE k4 < 3500 ORDER BY k1, k2, k3, k4 LIMIT 10 SETTINGS force_optimize_projection = 1)
    WHERE explain LIKE '%ReadFromMergeTree (p_narrow)%';

SELECT 'A8', count(), cityHash64(groupArray((k1, k2, k3, k4, narrow))) FROM
    (SELECT k1, k2, k3, k4, narrow FROM t_bp WHERE k4 < 3500 ORDER BY k1, k2, k3, k4 LIMIT 10);
SELECT 'A8', count(), cityHash64(groupArray((k1, k2, k3, k4, narrow))) FROM
    (SELECT k1, k2, k3, k4, narrow FROM t_bp WHERE k4 < 3500 ORDER BY k1, k2, k3, k4 LIMIT 10
     SETTINGS optimize_use_projections = 0);

-- The LIMIT cannot be reached: the projection covers every selected part and its whole selected
-- range holds fewer rows than the LIMIT, so neither plan stops early and it stays eligible.
SELECT 'A11', count() FROM (EXPLAIN actions = 1 SELECT k1, k2, k3, k4, narrow FROM t_bp
    WHERE k2 = 0 ORDER BY k1, k2, k3, k4 LIMIT 5000)
    WHERE explain LIKE '%ReadFromMergeTree (p_narrow)%';

DROP TABLE t_bp;

DROP TABLE IF EXISTS t_bp_full;

CREATE TABLE t_bp_full
(
    k1 String, k2 UInt32, k3 Bool, k4 UInt32, narrow UInt32,
    wide1 String, wide2 String, wide3 String,
    PROJECTION p_narrow (SELECT k1, k2, k3, k4, narrow ORDER BY k1, k2, k4),
    PROJECTION p_full (SELECT k1, k2, k3, k4, narrow ORDER BY k1, k2, k3, k4)
)
ENGINE = MergeTree ORDER BY (k1, k2, k3, k4)
SETTINGS index_granularity = 8192, index_granularity_bytes = 16384;

INSERT INTO t_bp_full SELECT 'a', intDiv(number, 500), (number % 3) = 0, number, number,
    repeat('w', 500), repeat('x', 500), repeat('y', 500) FROM numbers(4000);

SELECT 'A5', count() FROM (EXPLAIN actions = 1 SELECT k1, k2, k3, k4, narrow FROM t_bp_full
    WHERE k4 < 3500 ORDER BY k1, k2, k3, k4 LIMIT 10) WHERE explain LIKE '%ReadFromMergeTree (p_full)%';

DROP TABLE t_bp_full;

DROP TABLE IF EXISTS t_bp_fixed;

-- A key column pinned to a constant by the filter is skipped without contributing to the sorted
-- prefix, so the prefix length and the number of consumed key columns are different quantities here.
CREATE TABLE t_bp_fixed
(
    x UInt32, a UInt32, b UInt32, c UInt32, d UInt32, narrow UInt32,
    wide1 String, wide2 String, wide3 String,
    PROJECTION p_skip (SELECT x, a, b, c, d, narrow ORDER BY x, a, b, d)
)
ENGINE = MergeTree ORDER BY (x, a, b, c, d)
SETTINGS index_granularity = 8192, index_granularity_bytes = 16384;

INSERT INTO t_bp_fixed SELECT 1, intDiv(number, 500), number % 7, number % 3, number, number,
    repeat('w', 500), repeat('x', 500), repeat('y', 500) FROM numbers(4000);

SELECT 'A9', count() FROM (EXPLAIN actions = 1 SELECT x, a, b, c, d, narrow FROM t_bp_fixed
    WHERE x = 1 ORDER BY a, b, c, d LIMIT 10) WHERE explain LIKE '%ReadFromMergeTree (%.t_bp_fixed)%';

DROP TABLE t_bp_fixed;

DROP TABLE IF EXISTS t_bp_prune;

-- A projection whose leading key column is the filter's equality column prunes hard, yet its sorted
-- prefix for ORDER BY (a, b, d) is only (a, b): it is declined too, which is a deliberate cost.
CREATE TABLE t_bp_prune
(
    a UInt32, b UInt32, c UInt32, d UInt32, narrow UInt32,
    wide1 String, wide2 String, wide3 String,
    PROJECTION p_lead_c (SELECT a, b, c, d, narrow ORDER BY c, a, b)
)
ENGINE = MergeTree ORDER BY (a, b, d)
SETTINGS index_granularity = 8192, index_granularity_bytes = 16384;

INSERT INTO t_bp_prune SELECT intDiv(number, 500), number % 7, cityHash64(number) % 50, number, number,
    repeat('w', 500), repeat('x', 500), repeat('y', 500) FROM numbers(4000);

SELECT 'A10', count() FROM (EXPLAIN actions = 1 SELECT a, b, c, d, narrow FROM t_bp_prune
    WHERE c = 0 ORDER BY a, b, d LIMIT 10) WHERE explain LIKE '%ReadFromMergeTree (%.t_bp_prune)%';

SELECT 'A10', count(), cityHash64(groupArray((a, b, c, d, narrow))) FROM
    (SELECT a, b, c, d, narrow FROM t_bp_prune WHERE c = 0 ORDER BY a, b, d LIMIT 10);
SELECT 'A10', count(), cityHash64(groupArray((a, b, c, d, narrow))) FROM
    (SELECT a, b, c, d, narrow FROM t_bp_prune WHERE c = 0 ORDER BY a, b, d LIMIT 10
     SETTINGS optimize_use_projections = 0);

DROP TABLE t_bp_prune;

DROP TABLE IF EXISTS t_bp_partial;

-- The base key supplies only 3 of the 4 ORDER BY columns, so it does not eliminate the sort either
-- and the projection stays eligible however much shorter its own prefix is.
CREATE TABLE t_bp_partial
(
    a UInt32, b UInt32, c UInt32, e UInt32, narrow UInt32,
    wide1 String, wide2 String, wide3 String,
    PROJECTION p_ab (SELECT a, b, c, e, narrow ORDER BY a, b)
)
ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 8192, index_granularity_bytes = 16384;

INSERT INTO t_bp_partial SELECT intDiv(number, 500), number % 7, number % 3, number, number,
    repeat('w', 500), repeat('x', 500), repeat('y', 500) FROM numbers(4000);

SELECT 'A12', count() FROM (EXPLAIN actions = 1 SELECT a, b, c, e, narrow FROM t_bp_partial
    WHERE e < 3500 ORDER BY a, b, c, e LIMIT 10) WHERE explain LIKE '%ReadFromMergeTree (p_ab)%';

DROP TABLE t_bp_partial;

DROP TABLE IF EXISTS t_bp_eq;

-- Equal mark counts are adjudicated by the tie policy above, which keeps a projection that helps with
-- sorting; only a strictly cheaper candidate reaches the base-prefix comparison.
CREATE TABLE t_bp_eq
(
    k1 String, k2 UInt32, k3 Bool, k4 UInt32, narrow UInt32,
    PROJECTION p_narrow (SELECT k1, k2, k3, k4, narrow ORDER BY k1, k2, k4)
)
ENGINE = MergeTree ORDER BY (k1, k2, k3, k4)
SETTINGS index_granularity = 512;

INSERT INTO t_bp_eq SELECT 'a', intDiv(number, 500), (number % 3) = 0, number, number
    FROM numbers(4000);

SELECT 'A13', count() FROM (EXPLAIN actions = 1 SELECT k1, k2, k3, k4, narrow FROM t_bp_eq
    ORDER BY k1, k2, k3, k4 LIMIT 10) WHERE explain LIKE '%ReadFromMergeTree (p_narrow)%';

DROP TABLE t_bp_eq;
