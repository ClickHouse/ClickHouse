-- `ANY INNER JOIN` must return one row per matched KEY with every algorithm. `MergeJoin` used
-- to emit one row per matching left row (3 rows below instead of 1), so the result depended on
-- the algorithm - and on whether `SpillingHashJoin` spilled through the order-preserving
-- MergeJoin fallback. Now `MergeJoin` claims the key's globally first right row through shared
-- once-only flags (`JoinUsedFlags`), like the hash join does per hash-table entry.

SELECT '-- Duplicate left keys: one row per key with every algorithm';

SELECT count() FROM (SELECT 1 AS k, number AS v FROM numbers(3)) t1
ANY INNER JOIN (SELECT 1 AS k, number AS w FROM numbers(2)) t2 ON t1.k = t2.k
SETTINGS join_algorithm = 'hash';

SELECT count() FROM (SELECT 1 AS k, number AS v FROM numbers(3)) t1
ANY INNER JOIN (SELECT 1 AS k, number AS w FROM numbers(2)) t2 ON t1.k = t2.k
SETTINGS join_algorithm = 'partial_merge';

SELECT count() FROM (SELECT 1 AS k, number AS v FROM numbers(3)) t1
ANY INNER JOIN (SELECT 1 AS k, number AS w FROM numbers(2)) t2 ON t1.k = t2.k
SETTINGS join_algorithm = 'grace_hash';

SELECT '-- Right duplicates crossing the sorted right block boundaries';

-- 5 right rows per key with 7-row right blocks: most keys span two blocks, so the claim must
-- target the key's globally FIRST right row, or two probes of one key could both win.
SELECT count(), uniqExact(k) FROM (
    SELECT t1.k AS k FROM (SELECT number % 100 AS k FROM numbers(1000)) t1
    ANY INNER JOIN (SELECT number % 100 AS k FROM numbers(500)) t2 ON t1.k = t2.k
) SETTINGS join_algorithm = 'partial_merge', partial_merge_join_rows_in_right_blocks = 7;

SELECT '-- The spilled right side (external partial_merge)';

DROP TABLE IF EXISTS any_inner_l;
DROP TABLE IF EXISTS any_inner_r;
CREATE TABLE any_inner_l (t UInt32, id UInt32) ENGINE = MergeTree ORDER BY t;
INSERT INTO any_inner_l SELECT number, number % 20000 FROM numbers(100000);
CREATE TABLE any_inner_r (id UInt32, payload String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO any_inner_r SELECT number % 10000, concat('p', toString(number)) FROM numbers(50000);

SELECT count(), uniqExact(t) FROM (
    SELECT any_inner_l.t AS t FROM any_inner_l ANY INNER JOIN any_inner_r ON any_inner_l.id = any_inner_r.id
) SETTINGS join_algorithm = 'partial_merge', max_rows_in_join = 1000;

SELECT '-- The spilling hash join switched to the order-preserving MergeJoin';

SELECT count(), uniqExact(t), arraySort(groupArray(materialize(t))) = groupArray(materialize(t)) FROM (
    SELECT any_inner_l.t AS t FROM any_inner_l ANY INNER JOIN any_inner_r ON any_inner_l.id = any_inner_r.id
    ORDER BY any_inner_l.t
    SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 250000, max_bytes_ratio_before_external_join = 0,
             enable_analyzer = 1, enable_parallel_replicas = 0, optimize_read_in_order = 1,
             query_plan_read_in_order_through_join = 1, read_in_order_use_virtual_row = 1, query_plan_join_swap_table = 0
);

-- Concurrent probe streams racing for the claims: the counts must stay exact.
SELECT count(), uniqExact(t) FROM (
    SELECT any_inner_l.t AS t FROM any_inner_l ANY INNER JOIN any_inner_r ON any_inner_l.id = any_inner_r.id
    ORDER BY any_inner_l.t
    SETTINGS join_algorithm = 'parallel_hash', max_threads = 8,
             max_bytes_before_external_join = 250000, max_bytes_ratio_before_external_join = 0,
             enable_analyzer = 1, enable_parallel_replicas = 0, optimize_read_in_order = 1,
             query_plan_read_in_order_through_join = 1, read_in_order_use_virtual_row = 1, query_plan_join_swap_table = 0
);

DROP TABLE any_inner_l;
DROP TABLE any_inner_r;
