DROP TABLE IF EXISTS t_unusable_pk;
DROP TABLE IF EXISTS t_usable_pk;

CREATE TABLE t_unusable_pk (s String, v UInt32) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
INSERT INTO t_unusable_pk SELECT toString(number * 11), number FROM numbers(6);

CREATE TABLE t_usable_pk (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_usable_pk SELECT number, number FROM numbers(100);

SET enable_parallel_replicas = 1;
SET cluster_for_parallel_replicas = 'parallel_replicas';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 1;

-- A. The primary key cannot be used, so force_primary_key must reject the query. The replica-count
-- estimate runs first and must not report the row limit instead.
SET automatic_parallel_replicas_mode = 2;
SELECT v FROM t_unusable_pk WHERE startsWith(CAST(s, 'FixedString(40)'), '11') ORDER BY v
SETTINGS force_primary_key = 1, max_rows_to_read = 3; -- { serverError INDEX_NOT_USED }

-- B. The primary key is used and the read really does exceed max_rows_to_read, so the row limit is
-- still enforced.
SELECT v FROM t_usable_pk WHERE k >= 10 ORDER BY v
SETTINGS force_primary_key = 1, max_rows_to_read = 3; -- { serverError TOO_MANY_ROWS }

-- C. Same as A without parallel replicas.
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 0;
SELECT v FROM t_unusable_pk WHERE startsWith(CAST(s, 'FixedString(40)'), '11') ORDER BY v
SETTINGS force_primary_key = 1, max_rows_to_read = 3; -- { serverError INDEX_NOT_USED }

-- E. Same as A through automatic_parallel_replicas_mode = 1, which reaches the estimate only once
-- automatic_parallel_replicas_min_bytes_per_replica admits the read.
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 1;
SET automatic_parallel_replicas_min_bytes_per_replica = 0;
SELECT v FROM t_unusable_pk WHERE startsWith(CAST(s, 'FixedString(40)'), '11') ORDER BY v
SETTINGS force_primary_key = 1, max_rows_to_read = 3; -- { serverError INDEX_NOT_USED }

DROP TABLE t_unusable_pk;
DROP TABLE t_usable_pk;
