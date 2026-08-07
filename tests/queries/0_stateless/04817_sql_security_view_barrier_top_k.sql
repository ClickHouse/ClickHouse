-- A `SQL SECURITY DEFINER` / `NONE` view that can hide rows is a barrier for the top-K
-- rewrite too: `tryOptimizeTopK` installs a `__topKFilter` PREWHERE and minmax-based granule
-- skipping on the source, driven by the invoker's `ORDER BY ... LIMIT`, so below the view's
-- filtering it would make `read_rows` / timing depend on the rows the view hides.

-- Pin everything the plan shape and the `read_rows` comparison depend on: the test also runs
-- with randomized settings. `optimize_move_to_prewhere = 0` keeps the view's `WHERE` a
-- `FilterStep`, which is both the shape the top-K walk peels and a precondition of the dynamic
-- filter (it requires no PREWHERE). A single thread and the read-path injections pinned off
-- keep `read_rows` exactly reproducible; none of them affects what the barrier guards.
SET use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, query_plan_max_limit_for_top_k_optimization = 100,
    use_skip_indexes = 1, use_skip_indexes_on_data_read = 1,
    optimize_move_to_prewhere = 0, enable_parallel_replicas = 0, make_distributed_plan = 0,
    max_threads = 1,
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    page_cache_inject_eviction = 0;

DROP TABLE IF EXISTS t04817;
CREATE TABLE t04817 (key UInt64, value UInt64, owner String) ENGINE = MergeTree ORDER BY key;
INSERT INTO t04817 SELECT number, number * 7 % 10000, 'nobody' FROM numbers(10000);

CREATE VIEW v04817_invoker SQL SECURITY INVOKER AS SELECT * FROM t04817 WHERE owner != 'x';
CREATE VIEW v04817_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT * FROM t04817 WHERE owner != 'x';

-- The `INVOKER` view stays fully optimizable: the top-K dynamic filter is installed.
SELECT 'invoker filtering view gets top-K filter:', count() > 0 FROM (EXPLAIN actions = 1 SELECT * FROM v04817_invoker ORDER BY value LIMIT 3) WHERE explain LIKE '%__topKFilter%';

-- The filtering `DEFINER` view is a barrier: no top-K filtering driven from above it.
SELECT 'definer filtering view gets top-K filter:', count() FROM (EXPLAIN actions = 1 SELECT * FROM v04817_definer ORDER BY value LIMIT 3) WHERE explain LIKE '%__topKFilter%';

-- The barrier only drops the optimization, never the correctness of the result.
SELECT 'definer view results:', arraySort(groupArray(value)) = [0, 1, 2] FROM (SELECT value FROM v04817_definer ORDER BY value LIMIT 3);

DROP VIEW v04817_invoker;
DROP VIEW v04817_definer;
DROP TABLE t04817;

-- `read_rows` must not depend on the values of the rows the view hides. Twin tables, identical
-- except for the value of the single hidden row: the extreme minimum of the sort column in one,
-- unremarkable in the other. The `minmax` skip index on `value` is what the top-K rewrite would
-- prune granules with, so without the barrier the hidden extreme drags its granule to the front
-- of the ranking and the two reads diverge.
CREATE TABLE t04817_a (key UInt64, value UInt64, owner String, INDEX v_idx value TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1024;
CREATE TABLE t04817_b (key UInt64, value UInt64, owner String, INDEX v_idx value TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1024;

INSERT INTO t04817_a SELECT number, if(number = 50000, 0, 1000000 + number), if(number = 50000, 'hidden', 'nobody') FROM numbers(100001);
INSERT INTO t04817_b SELECT number, if(number = 50000, 1050000, 1000000 + number), if(number = 50000, 'hidden', 'nobody') FROM numbers(100001);

CREATE VIEW v04817_a DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT key, value FROM t04817_a WHERE owner != 'hidden';
CREATE VIEW v04817_b DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT key, value FROM t04817_b WHERE owner != 'hidden';

SELECT value FROM v04817_a ORDER BY value LIMIT 1 SETTINGS log_comment = '04817_probe_hidden_extreme';
SELECT value FROM v04817_b ORDER BY value LIMIT 1 SETTINGS log_comment = '04817_probe_hidden_plain';

SYSTEM FLUSH LOGS query_log;
-- `count() != 2` guards against the comparison passing vacuously on an empty match.
SELECT 'reading the view costs the same whatever the hidden value is:', multiIf(
        count() != 2, 'MISSING',
        anyIf(read_rows, log_comment = '04817_probe_hidden_extreme') = anyIf(read_rows, log_comment = '04817_probe_hidden_plain'),
        'same', 'DISCLOSED')
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND log_comment LIKE '04817_probe_%' AND type = 'QueryFinish';

DROP VIEW v04817_a;
DROP VIEW v04817_b;
DROP TABLE t04817_a;
DROP TABLE t04817_b;
