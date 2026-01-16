-- Tags: no-tsan, no-msan, no-asan, stateful
-- This test validates that max_execution_speed & timeout_before_checking_execution_speed works as expected with:
--      a) regular query
--      b) temp merge query
-- Our goal is to read enough data to trigger execution throttling (it won't work on small ans simple datasets), but not too much
-- to avoid unnecessarily long runtime.  Using of a local table, like test.hits or test.visits, is still too fast to trigger throttling.
-- This is why we use 'test.hist_s3' and sampling it heavily aiming for the shortest runtime.
-- NOTE: Normally test finishes in seconds, but accessing s3 still might hang. If becomes flaky again due to execution timeouts, try tag 'long'

SET max_execution_speed = 1000000;
SET timeout_before_checking_execution_speed = 0;

CREATE TEMPORARY TABLE times (t DateTime);

INSERT INTO times SELECT now();
SELECT count() FROM test.hits_s3 SAMPLE 1500000;
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 1 FROM times;
TRUNCATE TABLE times;

INSERT INTO times SELECT now();
SELECT count() FROM merge(test, '^hits_s3$') SAMPLE 1500000;
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 1 FROM times;
