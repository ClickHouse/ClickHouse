-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test checks the shape of the local query plan.

-- Lazy FINAL must not ignore SAMPLE: the result and `_sample_factor` must match regular FINAL.

DROP TABLE IF EXISTS t_lazy_final_sample;

CREATE TABLE t_lazy_final_sample (CounterID UInt32, UserID UInt64, version UInt64, s String)
ENGINE = ReplacingMergeTree(version)
ORDER BY (CounterID, intHash32(UserID))
SAMPLE BY intHash32(UserID);

SYSTEM STOP MERGES t_lazy_final_sample;

-- Two overlapping parts.
INSERT INTO t_lazy_final_sample SELECT number % 10, number, 1, toString(number % 10) FROM numbers(1000);
INSERT INTO t_lazy_final_sample SELECT number % 10, number, 2, toString(number % 10) FROM numbers(1000) WHERE number % 2 = 0;

SET query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0;

SELECT 'sample', count(), sum(_sample_factor) FROM t_lazy_final_sample FINAL SAMPLE 1 / 2 WHERE s = '7';
SELECT 'sample offset', count(), sum(_sample_factor) FROM t_lazy_final_sample FINAL SAMPLE 1 / 2 OFFSET 1 / 2 WHERE s = '7';
SELECT 'prewhere', count(), sum(_sample_factor) FROM t_lazy_final_sample FINAL SAMPLE 1 / 2 PREWHERE s = '7';

-- `SAMPLE 1` and an absolute size covering all rows read without sampling, so lazy FINAL still applies.
SELECT 'sample 1', count(), sum(_sample_factor) FROM t_lazy_final_sample FINAL SAMPLE 1 WHERE s = '7';
SELECT 'sample 1000000', count(), sum(_sample_factor) FROM t_lazy_final_sample FINAL SAMPLE 1000000 WHERE s = '7';

SELECT 'plan no sample', countIf(explain LIKE '%InputSelector%') > 0 FROM (EXPLAIN SELECT count() FROM t_lazy_final_sample FINAL WHERE s = '7');
SELECT 'plan sample 1', countIf(explain LIKE '%InputSelector%') > 0 FROM (EXPLAIN SELECT count() FROM t_lazy_final_sample FINAL SAMPLE 1 WHERE s = '7');
SELECT 'plan sample 1000000', countIf(explain LIKE '%InputSelector%') > 0 FROM (EXPLAIN SELECT count() FROM t_lazy_final_sample FINAL SAMPLE 1000000 WHERE s = '7');
SELECT 'plan sample 1/2', countIf(explain LIKE '%InputSelector%') > 0 FROM (EXPLAIN SELECT count() FROM t_lazy_final_sample FINAL SAMPLE 1 / 2 WHERE s = '7');

-- A part that does not overlap the others: the parts are split into non-intersecting and intersecting.
INSERT INTO t_lazy_final_sample SELECT 100 + number % 10, number, 1, toString(number % 10) FROM numbers(1000);

SELECT 'partial split', count(), sum(_sample_factor) FROM t_lazy_final_sample FINAL SAMPLE 1 / 2 WHERE s = '7';

-- All parts are non-intersecting: the whole read is replaced by a non-FINAL read that keeps SAMPLE.
SYSTEM START MERGES t_lazy_final_sample;
OPTIMIZE TABLE t_lazy_final_sample FINAL;

SELECT 'non-intersecting', count(), sum(_sample_factor) FROM t_lazy_final_sample FINAL SAMPLE 1 / 2 WHERE s = '7';

DROP TABLE t_lazy_final_sample;
