-- SummingMergeTree removes a row on merge only when ALL columns of the summation
-- sum up to zero. A FINAL read has to take the removal decision over the same full
-- set of columns even when the query itself reads only a subset of them (or none at
-- all, e.g. `SELECT count()`), otherwise rows silently disappear from the result.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=5c9f28ed5a2c585ad46bf74d6600cdcf3aa07a9e&name_0=NightlySQLancer&name_1=SQLancer

DROP TABLE IF EXISTS summing_final_subset;
CREATE TABLE summing_final_subset (c0 Int8, c1 Int32, c3 UInt64) ENGINE = SummingMergeTree ORDER BY c3;
SYSTEM STOP MERGES summing_final_subset;

-- Two overlapping parts; for keys 0..4 the column c0 sums to zero while c1 does not,
-- so no row may be removed.
INSERT INTO summing_final_subset SELECT 0, 1, number FROM numbers(10);
INSERT INTO summing_final_subset SELECT 1, 1, number + 5 FROM numbers(10);

SELECT count() FROM summing_final_subset FINAL;
SELECT count() FROM summing_final_subset FINAL SETTINGS do_not_merge_across_partitions_select_final = 1;
SELECT c0, c3 FROM summing_final_subset FINAL ORDER BY c3;
SELECT sum(c1) FROM summing_final_subset FINAL;

-- A summed column used only by PREWHERE has to survive the PREWHERE projection until
-- FINAL decides whether the resulting row is all zero. Test both analyzers.
SELECT count() FROM summing_final_subset FINAL PREWHERE c1 IN (1, 2) SETTINGS enable_analyzer = 0;
SELECT count() FROM summing_final_subset FINAL PREWHERE c1 IN (1, 2) SETTINGS enable_analyzer = 1;
SELECT count() FROM summing_final_subset FINAL WHERE c1 IN (1, 2) SETTINGS optimize_move_to_prewhere_if_final = 1;

-- The FINAL read must agree with the state after a real merge.
SYSTEM START MERGES summing_final_subset;
OPTIMIZE TABLE summing_final_subset FINAL;
SELECT count() FROM summing_final_subset FINAL;
SELECT count() FROM summing_final_subset;

DROP TABLE summing_final_subset;

-- The opposite direction: when ALL columns of the summation sum up to zero, the row
-- is removed by the merge, and a FINAL read must remove it too - also when the query
-- reads none of the summed columns.
DROP TABLE IF EXISTS summing_final_zero;
CREATE TABLE summing_final_zero (c0 Int8, c1 Int32, c3 UInt64) ENGINE = SummingMergeTree ORDER BY c3;
SYSTEM STOP MERGES summing_final_zero;

INSERT INTO summing_final_zero SELECT 1, 1, number FROM numbers(5);
INSERT INTO summing_final_zero SELECT -1, -1, number FROM numbers(5);

SELECT count() FROM summing_final_zero FINAL;
SELECT c3 FROM summing_final_zero FINAL ORDER BY c3;

SYSTEM START MERGES summing_final_zero;
OPTIMIZE TABLE summing_final_zero FINAL;
SELECT count() FROM summing_final_zero FINAL;
SELECT count() FROM summing_final_zero;

DROP TABLE summing_final_zero;

-- With an explicit list of columns to sum, only the listed columns decide the removal:
-- for keys 0..4 the listed c1 sums to zero, so the rows are removed even though the
-- unlisted c0 does not.
DROP TABLE IF EXISTS summing_final_listed;
CREATE TABLE summing_final_listed (c0 Int8, c1 Int32, c3 UInt64) ENGINE = SummingMergeTree(c1) ORDER BY c3;
SYSTEM STOP MERGES summing_final_listed;

INSERT INTO summing_final_listed SELECT 5, 1, number FROM numbers(10);
INSERT INTO summing_final_listed SELECT 7, -1, number FROM numbers(5);

SELECT count() FROM summing_final_listed FINAL;
SELECT c0, c3 FROM summing_final_listed FINAL ORDER BY c3;

SYSTEM START MERGES summing_final_listed;
OPTIMIZE TABLE summing_final_listed FINAL;
SELECT count() FROM summing_final_listed FINAL;
SELECT count() FROM summing_final_listed;

DROP TABLE summing_final_listed;
