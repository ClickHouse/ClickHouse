-- `RuntimeFilterIndexAnalysis::supportsDataType` records a `[min, max]` key range only for integer and
-- date-like keys, while the build side happily collects a runtime filter for a `String` key. Once such a
-- filter overflows `join_runtime_filter_exact_values_limit` it keeps no exact values either, so there is
-- nothing left a pruning predicate could be built from - however index-capable the probe side is.
-- The read-time path must then be a true no-op instead of materializing a predicate for every part.

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

DROP TABLE IF EXISTS probe_string_pk;
DROP TABLE IF EXISTS probe_string_set;
DROP TABLE IF EXISTS probe_int_pk;
DROP TABLE IF EXISTS build_string_small;
DROP TABLE IF EXISTS build_string_large;
DROP TABLE IF EXISTS build_int_large;

CREATE TABLE probe_string_pk (k String, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
CREATE TABLE probe_string_set (k UInt64, s String, INDEX idx_s s TYPE set(0) GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
CREATE TABLE probe_int_pk (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
CREATE TABLE build_string_small (k String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE build_string_large (k String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE build_int_large (k UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO probe_string_pk SELECT leftPad(toString(number), 6, '0'), number FROM numbers(10000);
INSERT INTO probe_string_set SELECT number, leftPad(toString(number), 6, '0') FROM numbers(10000);
INSERT INTO probe_int_pk SELECT number, number FROM numbers(10000);
INSERT INTO build_string_small SELECT leftPad(toString(number * 1000), 6, '0') FROM numbers(5);
INSERT INTO build_string_large SELECT leftPad(toString(number), 6, '0') FROM numbers(5000);
INSERT INTO build_int_large SELECT number FROM numbers(5000);

-- A small build side keeps the exact values, so a `String` key prunes as usual, both through the
-- primary key and through a `set` index. These are the controls that prove the queries below are
-- shaped so that pruning would happen if it were possible at all.
SELECT count(), sum(v) FROM probe_string_pk AS p INNER JOIN build_string_small AS b ON p.k = b.k
    SETTINGS log_comment = '05247_string_pk_exact';
SELECT count(), sum(k) FROM probe_string_set AS p INNER JOIN build_string_small AS b ON p.s = b.k
    SETTINGS log_comment = '05247_string_set_exact';

-- The overflowed `String` filter: no exact values, and no range is ever recorded for a `String` key.
SELECT count(), sum(v) FROM probe_string_pk AS p INNER JOIN build_string_large AS b ON p.k = b.k
    SETTINGS join_runtime_filter_exact_values_limit = 1000, log_comment = '05247_string_pk_overflowed';
SELECT count(), sum(k) FROM probe_string_set AS p INNER JOIN build_string_large AS b ON p.s = b.k
    SETTINGS join_runtime_filter_exact_values_limit = 1000, log_comment = '05247_string_set_overflowed';

-- The control for the overflow itself: an integer key of the very same size still records a range,
-- so overflowing the exact values does not make it a no-op.
SELECT count(), sum(v) FROM probe_int_pk AS p INNER JOIN build_int_large AS b ON p.k = b.k
    SETTINGS join_runtime_filter_exact_values_limit = 1000, log_comment = '05247_int_pk_overflowed';

SYSTEM FLUSH LOGS query_log;
-- `considered` must be zero for both overflowed `String` cases: nothing can be pruned, so nothing may
-- be built per part either.
SELECT 'granules considered and dropped';
SELECT
    log_comment,
    argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time) > 0,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('05247_string_pk_exact', '05247_string_set_exact', '05247_string_pk_overflowed',
        '05247_string_set_overflowed', '05247_int_pk_overflowed')
    AND type = 'QueryFinish'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE probe_string_pk;
DROP TABLE probe_string_set;
DROP TABLE probe_int_pk;
DROP TABLE build_string_small;
DROP TABLE build_string_large;
DROP TABLE build_int_large;
