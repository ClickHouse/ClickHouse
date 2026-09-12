-- A `SQL SECURITY DEFINER` / `NONE` view that can hide rows is a barrier for projection
-- planning too: `QueryDAG::build` in `projectionsCommon.cpp` collects every filter of the
-- chain below the aggregation - the invoker's predicates together with the view's own
-- filtering - and both aggregate-projection candidate analysis and normal-projection part
-- filtering prune parts and marks with it, so below the view's filtering they would make
-- `read_rows` / timing depend on the rows the view hides.

-- Pin everything the plan shape and the `read_rows` comparison depend on: the test also runs
-- with randomized settings. `optimize_move_to_prewhere = 0` keeps the invoker's predicate a
-- `FilterStep`, the shape the projection walk collects. A single thread and the read-path
-- injections pinned off keep `read_rows` exactly reproducible; none of them affects what the
-- barrier guards.
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1,
    optimize_move_to_prewhere = 0, enable_parallel_replicas = 0, make_distributed_plan = 0,
    max_threads = 1,
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    page_cache_inject_eviction = 0;

DROP TABLE IF EXISTS t04821;
CREATE TABLE t04821
(
    key UInt64,
    value UInt64,
    owner String,
    PROJECTION p_ord (SELECT key, value, owner ORDER BY value),
    PROJECTION p_agg (SELECT owner, count() GROUP BY owner)
)
ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1024;
INSERT INTO t04821 SELECT number, number * 7 % 10000, 'nobody' FROM numbers(10000);

CREATE VIEW v04821_invoker SQL SECURITY INVOKER AS SELECT * FROM t04821 WHERE owner != 'x';
CREATE VIEW v04821_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT * FROM t04821 WHERE owner != 'x';

-- The `INVOKER` view stays fully optimizable: the invoker's predicate selects the normal
-- projection, and the invoker's aggregation selects the aggregate projection.
SELECT 'invoker view uses normal projection:', count() > 0 FROM (EXPLAIN SELECT count() FROM v04821_invoker WHERE value = 5) WHERE explain LIKE '%p_ord%';
SELECT 'invoker view uses aggregate projection:', count() > 0 FROM (EXPLAIN SELECT owner, count() FROM v04821_invoker GROUP BY owner) WHERE explain LIKE '%p_agg%';

-- The filtering `DEFINER` view is a barrier: no projection planning driven from above it.
SELECT 'definer view uses normal projection:', count() FROM (EXPLAIN SELECT count() FROM v04821_definer WHERE value = 5) WHERE explain LIKE '%p_ord%';
SELECT 'definer view uses aggregate projection:', count() FROM (EXPLAIN SELECT owner, count() FROM v04821_definer GROUP BY owner) WHERE explain LIKE '%p_agg%';

-- The barrier only drops the optimization, never the correctness of the result.
SELECT 'definer view results:', count() = 1, sum(value) = 5000 FROM (SELECT value FROM v04821_definer WHERE value = 5000 AND key = 5000);
SELECT 'definer view aggregate results:', count() = 1 FROM (SELECT owner, count() AS c FROM v04821_definer GROUP BY owner HAVING c = 10000);

DROP VIEW v04821_invoker;
DROP VIEW v04821_definer;
DROP TABLE t04821;

-- `read_rows` must not depend on the values of the rows the view hides. Twin tables, identical
-- except for the value of the single hidden row: matching the invoker's predicate in one,
-- unremarkable in the other. The normal projection ordered by `value` is what the projection
-- optimization would prune marks with, so without the barrier the hidden matching row drags its
-- granule into the read and the two reads diverge.
CREATE TABLE t04821_a (key UInt64, value UInt64, owner String, PROJECTION p_ord (SELECT key, value, owner ORDER BY value))
ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1024;
CREATE TABLE t04821_b (key UInt64, value UInt64, owner String, PROJECTION p_ord (SELECT key, value, owner ORDER BY value))
ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1024;

INSERT INTO t04821_a SELECT number, if(number = 50000, 0, 1000000 + number), if(number = 50000, 'hidden', 'nobody') FROM numbers(100001);
INSERT INTO t04821_b SELECT number, if(number = 50000, 1050000, 1000000 + number), if(number = 50000, 'hidden', 'nobody') FROM numbers(100001);

CREATE VIEW v04821_a DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT key, value FROM t04821_a WHERE owner != 'hidden';
CREATE VIEW v04821_b DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT key, value FROM t04821_b WHERE owner != 'hidden';

SELECT count() FROM v04821_a WHERE value < 1000 SETTINGS log_comment = '04821_probe_hidden_match';
SELECT count() FROM v04821_b WHERE value < 1000 SETTINGS log_comment = '04821_probe_hidden_plain';

SYSTEM FLUSH LOGS query_log;
-- `count() != 2` guards against the comparison passing vacuously on an empty match.
SELECT 'reading the view costs the same whatever the hidden value is:', multiIf(
        count() != 2, 'MISSING',
        anyIf(read_rows, log_comment = '04821_probe_hidden_match') = anyIf(read_rows, log_comment = '04821_probe_hidden_plain'),
        'same', 'DISCLOSED')
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND log_comment LIKE '04821_probe_%' AND type = 'QueryFinish';

DROP VIEW v04821_a;
DROP VIEW v04821_b;
DROP TABLE t04821_a;
DROP TABLE t04821_b;
