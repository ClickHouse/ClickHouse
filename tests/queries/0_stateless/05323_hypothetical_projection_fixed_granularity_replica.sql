-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: SharedMergeTree replaces the replicated engine and has no replica with fixed granularity
-- a replica with fixed granularity makes the table write fixed marks, so the row widths do not change the projection layout

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_fixed_r1;
DROP TABLE IF EXISTS t_fixed_r2;

CREATE TABLE t_fixed_r1 (a UInt64, b UInt64, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05323/t', 'r1') ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_fixed_r2 (a UInt64, b UInt64, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05323/t', 'r2') ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, enable_mixed_granularity_parts = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_fixed_r2 SELECT number, number % 100, repeat('x', number % 50) FROM numbers(1000);

CREATE HYPOTHETICAL PROJECTION p_b ON t_fixed_r2 (SELECT a, b, s ORDER BY b);
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a, b, s FROM t_fixed_r2 WHERE b < 50)
WHERE match(trim(explain), '^(status|verdict):');
DROP HYPOTHETICAL PROJECTION p_b ON t_fixed_r2;

-- the real projection on the same table, with the read that the optimizer selects
ALTER TABLE t_fixed_r2 ADD PROJECTION p_b (SELECT a, b, s ORDER BY b);
ALTER TABLE t_fixed_r2 MATERIALIZE PROJECTION p_b SETTINGS mutations_sync = 2;
SELECT 'real projection marks', marks FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_fixed_r2' AND active;
SELECT if(explain LIKE '%ReadFromMergeTree (p_b)%', 'real: from the projection', 'real: from the base table')
FROM (EXPLAIN SELECT a, b, s FROM t_fixed_r2 WHERE b < 50) WHERE explain LIKE '%ReadFromMergeTree%';

DROP TABLE t_fixed_r1;
DROP TABLE t_fixed_r2;
