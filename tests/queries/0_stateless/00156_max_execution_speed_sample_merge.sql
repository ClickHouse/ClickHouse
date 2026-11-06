-- Tags: stateful
-- This test validates that max_execution_speed & timeout_before_checking_execution_speed works with:
--      a) regular query
--      b) temp merge query
-- NOTE: This test uses simple synthetic data to validate the fact throttling was applied.
-- If throttling works as expected - each execution will take >= 1 second, as we allow not more than {max_execution_speed} records/seconds
-- If it doesn't - each select will finish immediately, and the test will fail
-- NOTE: Setting max_block_size=1 to ensure sleepEachRow(..) applies per each row guaranteed and the resulting timing is predictable [2-3] seconds

SET max_execution_speed = 10;
SET timeout_before_checking_execution_speed = 0;
SET max_block_size = 1;

CREATE TEMPORARY TABLE times (t DateTime);

DROP TABLE IF EXISTS t00156_max_execution_speed_sample_merge;
CREATE TABLE t00156_max_execution_speed_sample_merge (v UInt64);
INSERT INTO t00156_max_execution_speed_sample_merge SELECT number FROM numbers(28);

INSERT INTO times SELECT now();
SELECT * FROM t00156_max_execution_speed_sample_merge WHERE sleepEachRow(0.1) == 0 FORMAT Null;
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 1 FROM times;
TRUNCATE TABLE times;

INSERT INTO times SELECT now();
SELECT * FROM t00156_max_execution_speed_sample_merge WHERE sleepEachRow(0.1) == 0 FORMAT Null;
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 1 FROM times;
