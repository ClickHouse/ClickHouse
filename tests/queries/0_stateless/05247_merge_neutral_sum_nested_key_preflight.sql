-- A nested dictionary key is not eight stored bytes per row. Its compact
-- part's unrelated payload must not justify passing the default byte gate.
SET max_threads = 2;
SET allow_suspicious_low_cardinality_types = 1;
SET optimize_merge_neutral_sum_children_min_read_bytes = DEFAULT;
SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';
DROP TABLE IF EXISTS t05247_merge;
DROP TABLE IF EXISTS t05247_keys;
CREATE TABLE t05247_keys
(
    k Tuple(Tuple(LowCardinality(UInt64))),
    d UInt64,
    PROJECTION p (SELECT k, sum(d) GROUP BY k)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 1000000, min_bytes_for_wide_part = 100000000;
INSERT INTO t05247_keys SELECT tuple(tuple(number % 10)), 1 FROM numbers(400000);
CREATE TABLE t05247_merge
(k Tuple(Tuple(LowCardinality(UInt64))), pnl Nullable(Float64))
ENGINE = Merge(currentDatabase(), '^t05247_keys$');
SELECT arraySort(groupArray(tuple(k, isNull(s)))) FROM
(SELECT k, sum(pnl) AS s FROM t05247_merge GROUP BY k)
SETTINGS optimize_merge_neutral_sum_children = 0;
SELECT arraySort(groupArray(tuple(k, isNull(s)))) FROM
(SELECT k, sum(pnl) AS s FROM t05247_merge GROUP BY k)
SETTINGS optimize_merge_neutral_sum_children = 1, log_comment = 't05247_default';
SELECT arraySort(groupArray(tuple(k, isNull(s)))) FROM
(SELECT k, sum(pnl) AS s FROM t05247_merge GROUP BY k)
SETTINGS optimize_merge_neutral_sum_children = 1,
    optimize_merge_neutral_sum_children_min_read_bytes = 0, log_comment = 't05247_override';
SYSTEM FLUSH LOGS;
SELECT log_comment, empty(projections) FROM system.user_query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('t05247_default', 't05247_override')
ORDER BY log_comment;
DROP TABLE t05247_merge;
DROP TABLE t05247_keys;
