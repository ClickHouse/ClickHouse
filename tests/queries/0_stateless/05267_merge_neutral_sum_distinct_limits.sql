-- Internal key deduplication must not inherit caps for a user `DISTINCT`.
SET max_threads = 2;
SET optimize_merge_neutral_sum_children_min_read_bytes = 0;
SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';
DROP TABLE IF EXISTS t05267_merge;
DROP TABLE IF EXISTS t05267_keys;
CREATE TABLE t05267_keys
(k UInt64, d UInt64, PROJECTION p (SELECT k, sum(d) GROUP BY k))
ENGINE = MergeTree ORDER BY k;
INSERT INTO t05267_keys SELECT number % 10, 1 FROM numbers(10000);
CREATE TABLE t05267_merge (k UInt64, pnl Nullable(Float64))
ENGINE = Merge(currentDatabase(), '^t05267_keys$');
SELECT arraySort(groupArray(tuple(k, isNull(s)))) FROM
(SELECT k, sum(pnl) AS s FROM t05267_merge GROUP BY k)
SETTINGS optimize_merge_neutral_sum_children = 0, max_rows_in_distinct = 1, max_bytes_in_distinct = 1;
SELECT arraySort(groupArray(tuple(k, isNull(s)))) FROM
(SELECT k, sum(pnl) AS s FROM t05267_merge GROUP BY k)
SETTINGS optimize_merge_neutral_sum_children = 1, max_rows_in_distinct = 1,
    distinct_overflow_mode = 'throw', log_comment = 't05267_rows';
SELECT arraySort(groupArray(tuple(k, isNull(s)))) FROM
(SELECT k, sum(pnl) AS s FROM t05267_merge GROUP BY k)
SETTINGS optimize_merge_neutral_sum_children = 1, max_bytes_in_distinct = 1,
    distinct_overflow_mode = 'throw', log_comment = 't05267_bytes';
SELECT arraySort(groupArray(tuple(k, isNull(s)))) FROM
(SELECT k, sum(pnl) AS s FROM t05267_merge GROUP BY k)
SETTINGS optimize_merge_neutral_sum_children = 1, max_rows_in_distinct = 1, max_bytes_in_distinct = 1,
    distinct_overflow_mode = 'break', log_comment = 't05267_break';
SYSTEM FLUSH LOGS;
SELECT log_comment, notEmpty(projections) FROM system.user_query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('t05267_rows', 't05267_bytes', 't05267_break')
ORDER BY log_comment;
DROP TABLE t05267_merge;
DROP TABLE t05267_keys;
