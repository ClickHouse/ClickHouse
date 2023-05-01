-- Tags: no-fasttest

SET min_execution_speed = 100000000000, timeout_before_checking_execution_speed = 0;
SELECT count() FROM system.numbers; -- { serverError 160 }
SELECT 'Ok (1)';
SET min_execution_speed = 0;

SET min_execution_speed_bytes = 800000000000, timeout_before_checking_execution_speed = 0;
SELECT count() FROM system.numbers; -- { serverError 160 }
SELECT 'Ok (2)';
SET min_execution_speed_bytes = 0;

SET max_execution_speed = 1000000;
SET max_block_size = 100;

CREATE TEMPORARY TABLE times (t DateTime);

INSERT INTO times SELECT now();
SELECT count() FROM numbers(2000000);
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 1 FROM times;
SELECT 'Ok (3)';
SET max_execution_speed = 0;

SET max_execution_speed_bytes = 8000000;
TRUNCATE TABLE times;

INSERT INTO times SELECT now();
SELECT count() FROM numbers(2000000);
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 1 FROM times;
SELECT 'Ok (4)';
SET max_execution_speed_bytes = 0;

-- Note that 'min_execution_speed' does not count sleeping due to throttling
-- with 'max_execution_speed' and similar limits like 'priority' and 'max_network_bandwidth'

-- Note: I have to disable this part of the test because it actually can work slower under sanitizers,
-- with debug builds and in presense of random system hickups in our CI environment.

--SET max_execution_speed = 1000000, min_execution_speed = 2000000;
-- And this query will work despite the fact that the above settings look contradictory.
--SELECT count() FROM numbers(1000000);
--SELECT 'Ok (5)';
