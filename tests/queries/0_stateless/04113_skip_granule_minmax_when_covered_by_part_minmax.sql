-- Tags: no-parallel-replicas
-- no-parallel-replicas: the replicas evaluate the index, and their profile events are not in the query log of the initiator.

-- A skip index over partition key columns is not evaluated on the granules of a part whose part-level minmax index
-- already proves its condition, both in the index analysis and on data read.

DROP TABLE IF EXISTS t_skip_granule_minmax;

-- `indexHint` puts all rows into one partition, while the part-level minmax index still covers `c`.
CREATE TABLE t_skip_granule_minmax (c Int32, v Int32, INDEX i c TYPE minmax)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY indexHint(c)
SETTINGS index_granularity = 1;

INSERT INTO t_skip_granule_minmax SELECT 123 + number, number % 7 FROM numbers(1000);

SET use_skip_indexes = 1, use_query_condition_cache = 0;

-- Each condition with the index evaluated on analysis and on data read, and the reference without skip indexes.
SELECT 'covered', count() FROM t_skip_granule_minmax WHERE c > 100 SETTINGS use_skip_indexes_on_data_read = 0, log_comment = 'part_minmax_covered_0';
SELECT 'covered', count() FROM t_skip_granule_minmax WHERE c > 100 SETTINGS use_skip_indexes_on_data_read = 1, log_comment = 'part_minmax_covered_1';
SELECT 'covered', count() FROM t_skip_granule_minmax WHERE c > 100 SETTINGS use_skip_indexes = 0;

-- The condition on `v` is unknown to the index; in a conjunction it does not prevent the proof.
SELECT 'covered and other column', count() FROM t_skip_granule_minmax WHERE c > 100 AND v > 3 SETTINGS use_skip_indexes_on_data_read = 0, log_comment = 'part_minmax_and_0';
SELECT 'covered and other column', count() FROM t_skip_granule_minmax WHERE c > 100 AND v > 3 SETTINGS use_skip_indexes_on_data_read = 1, log_comment = 'part_minmax_and_1';
SELECT 'covered and other column', count() FROM t_skip_granule_minmax WHERE c > 100 AND v > 3 SETTINGS use_skip_indexes = 0;

-- Under `OR` the unknown condition on `v` cannot be taken as true, so the index is evaluated.
SELECT 'or other column', count() FROM t_skip_granule_minmax WHERE c = 123 OR v = 0 SETTINGS use_skip_indexes_on_data_read = 0, log_comment = 'part_minmax_or_0';
SELECT 'or other column', count() FROM t_skip_granule_minmax WHERE c = 123 OR v = 0 SETTINGS use_skip_indexes_on_data_read = 1, log_comment = 'part_minmax_or_1';
SELECT 'or other column', count() FROM t_skip_granule_minmax WHERE c = 123 OR v = 0 SETTINGS use_skip_indexes = 0;

-- Partially covered: the index is evaluated.
SELECT 'partially covered', count() FROM t_skip_granule_minmax WHERE c > 500 SETTINGS use_skip_indexes_on_data_read = 0, log_comment = 'part_minmax_partial_0';
SELECT 'partially covered', count() FROM t_skip_granule_minmax WHERE c > 500 SETTINGS use_skip_indexes_on_data_read = 1, log_comment = 'part_minmax_partial_1';
SELECT 'partially covered', count() FROM t_skip_granule_minmax WHERE c > 500 SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['SkipIndexEvaluationsAvoidedByPartMinMax'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'part\_minmax\_%'
ORDER BY log_comment;

DROP TABLE t_skip_granule_minmax;
