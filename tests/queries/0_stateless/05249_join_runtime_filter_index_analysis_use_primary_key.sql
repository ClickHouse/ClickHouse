-- `use_primary_key = 0` switches off every pruning by the primary key, and the read-time pruning by a
-- join runtime filter (`enable_join_runtime_filters_index_analysis`) is no exception: a join key that
-- is a primary key column is then pruned only by a skip index covering it. Without such an index the
-- probe read registers no descriptor, so the build side does not track the key range either.

SET explain_query_plan_default = 'legacy'; -- the `Key range tracking` line is printed by the non-pretty EXPLAIN
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET join_algorithm = 'hash';
SET max_bytes_ratio_before_external_join = 0;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET max_insert_threads = 1;

DROP TABLE IF EXISTS probe_pk;
DROP TABLE IF EXISTS probe_pk_minmax;
DROP TABLE IF EXISTS build;

CREATE TABLE probe_pk (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
CREATE TABLE probe_pk_minmax (k UInt64, v UInt64, INDEX idx_k k TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
CREATE TABLE build (k UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO probe_pk SELECT number, number FROM numbers(10000);
INSERT INTO probe_pk_minmax SELECT number, number FROM numbers(10000);
INSERT INTO build SELECT number + 5000 FROM numbers(50);

SELECT 'key range tracking';
SELECT 'pk, use_primary_key = 1', trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM probe_pk AS p INNER JOIN build AS b ON p.k = b.k
    SETTINGS use_primary_key = 1
) WHERE explain LIKE '%Key range tracking%';
SELECT 'pk, use_primary_key = 0', trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM probe_pk AS p INNER JOIN build AS b ON p.k = b.k
    SETTINGS use_primary_key = 0
) WHERE explain LIKE '%Key range tracking%';
SELECT 'pk + minmax, use_primary_key = 0', trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM probe_pk_minmax AS p INNER JOIN build AS b ON p.k = b.k
    SETTINGS use_primary_key = 0
) WHERE explain LIKE '%Key range tracking%';

SELECT 'results';
SELECT count(), sum(p.v) FROM probe_pk AS p INNER JOIN build AS b ON p.k = b.k
    SETTINGS use_primary_key = 1, log_comment = '05249_pk_on';
SELECT count(), sum(p.v) FROM probe_pk AS p INNER JOIN build AS b ON p.k = b.k
    SETTINGS use_primary_key = 0, log_comment = '05249_pk_off';
SELECT count(), sum(p.v) FROM probe_pk_minmax AS p INNER JOIN build AS b ON p.k = b.k
    SETTINGS use_primary_key = 0, log_comment = '05249_pk_off_minmax';

SYSTEM FLUSH LOGS query_log;
SELECT 'granules considered and dropped';
SELECT
    log_comment,
    argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time) > 0,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('05249_pk_on', '05249_pk_off', '05249_pk_off_minmax')
    AND type = 'QueryFinish'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE probe_pk;
DROP TABLE probe_pk_minmax;
DROP TABLE build;
