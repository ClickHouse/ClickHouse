-- A preliminary `DISTINCT` that reads directly from a source coalesces its output before a parallel final
-- `DISTINCT` scatters it. The delay is bounded by the consumed input rows, so an unbounded input that adds
-- no new keys still lets a downstream limit stop the query.

SET allow_parallel_distinct = 1, max_threads = 4, max_block_size = 128;
SET max_bytes_before_external_distinct = 0, max_bytes_ratio_before_external_distinct = 0;
SET optimize_distinct_in_order = 0, query_plan_remove_redundant_distinct = 0;
SET max_execution_time = 120;

SELECT count() FROM (SELECT * FROM (SELECT DISTINCT number % 3 AS k FROM system.numbers_mt) LIMIT 3);

-- Highly duplicated input, where most chunks add no new keys.
DROP TABLE IF EXISTS t_coalesced_distinct;
CREATE TABLE t_coalesced_distinct (k UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_coalesced_distinct SELECT number, toString(number % 1000) FROM numbers(200000);

SELECT count(), sum(toUInt64(s)) FROM (SELECT DISTINCT s FROM t_coalesced_distinct);
SELECT count(), sum(toUInt64(s)) FROM (SELECT DISTINCT s FROM t_coalesced_distinct) SETTINGS allow_parallel_distinct = 0;

DROP TABLE t_coalesced_distinct;
