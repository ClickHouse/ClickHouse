-- Tags: no-parallel-replicas
-- https://github.com/ClickHouse/ClickHouse/issues/106561

SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 2;
SET cluster_for_parallel_replicas = 'parallel_replicas';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 1;

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t0 VALUES (1);

SELECT * FROM t0 SETTINGS asterisk_include_virtual_columns = 1 FORMAT Null;

DROP TABLE t0;
