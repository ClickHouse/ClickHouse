-- `EXPLAIN ESTIMATE` of a lazy `FINAL` query counts the fallback `FINAL` read too,
-- although that read is analyzed and built only when the query uses it.

SET use_skip_indexes = 1;
SET use_skip_indexes_if_final = 1;
SET use_skip_indexes_if_final_exact_mode = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET enable_parallel_replicas = 0;
SET min_filtered_ratio_for_lazy_final = 0;
-- Inserted parts get level 1, so `FINAL` can read the part that does not intersect the others without merging.
SET optimize_on_insert = 1;

DROP TABLE IF EXISTS t_lazy_final_estimate;

CREATE TABLE t_lazy_final_estimate
(
    key UInt64,
    ver UInt64,
    v UInt8,
    INDEX idx_v v TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY key
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_lazy_final_estimate;

-- Four parts with interleaved keys, and one part that does not intersect them.
INSERT INTO t_lazy_final_estimate SELECT number * 4 + 0, 1, intHash64(number * 4 + 0) % 127 = 0 FROM numbers(1000);
INSERT INTO t_lazy_final_estimate SELECT number * 4 + 1, 1, intHash64(number * 4 + 1) % 127 = 0 FROM numbers(1000);
INSERT INTO t_lazy_final_estimate SELECT number * 4 + 2, 1, intHash64(number * 4 + 2) % 127 = 0 FROM numbers(1000);
INSERT INTO t_lazy_final_estimate SELECT number * 4 + 3, 1, intHash64(number * 4 + 3) % 127 = 0 FROM numbers(1000);
INSERT INTO t_lazy_final_estimate SELECT 100000 + number, 1, intHash64(number) % 101 = 0 FROM numbers(1000);

SELECT 'without lazy FINAL';
SELECT parts, rows, marks FROM (EXPLAIN ESTIMATE SELECT count() FROM t_lazy_final_estimate FINAL WHERE v = 1 SETTINGS query_plan_optimize_lazy_final = 0);
SELECT parts, rows, marks FROM (EXPLAIN ESTIMATE SELECT count() FROM t_lazy_final_estimate FINAL WHERE v = 1 AND key < 50000 SETTINGS query_plan_optimize_lazy_final = 0);

-- The set-building read, the fallback `FINAL` read, and the read of the non-intersecting part.
SELECT 'with lazy FINAL';
SELECT parts, rows, marks FROM (EXPLAIN ESTIMATE SELECT count() FROM t_lazy_final_estimate FINAL WHERE v = 1 SETTINGS query_plan_optimize_lazy_final = 1);
SELECT parts, rows, marks FROM (EXPLAIN ESTIMATE SELECT count() FROM t_lazy_final_estimate FINAL WHERE v = 1 AND key < 50000 SETTINGS query_plan_optimize_lazy_final = 1);

DROP TABLE t_lazy_final_estimate;
