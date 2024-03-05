DROP TABLE IF EXISTS row_limits_test;

SET max_block_size = 10;
SET max_rows_to_read = 20;
SET read_overflow_mode = 'throw';

SELECT count() FROM numbers(30); -- { serverError 158 }
SELECT count() FROM numbers(19);
SELECT count() FROM numbers(20);
SELECT count() FROM numbers(21); -- { serverError 158 }

-- check early exception if the estimated number of rows is high
SELECT * FROM numbers(30); -- { serverError 158 }

SET read_overflow_mode = 'break';

SELECT count() FROM numbers(19);
SELECT count() FROM numbers(20);
SELECT count() FROM numbers(21);
SELECT count() FROM numbers(29);
SELECT count() FROM numbers(30);
SELECT count() FROM numbers(31);

-- check that partial result is returned even if the estimated number of rows is high
SELECT * FROM numbers(30);

-- the same for uneven block sizes
SET max_block_size = 11;
SELECT * FROM numbers(30);
SET max_block_size = 9;
SELECT * FROM numbers(30);

-- When reaching row limits, make sure we don't do a large amount of range scans and continue
-- processing all parts when we don't need to. For instance, we create 2 parts below with 30 rows in each.
-- If we have a row limit <= 30, we shouldn't exceed this value when max_threads = 1.
-- (process_part in MergeTreeDataSelectExecutor uses a thread pool the size of max_threads to read data,
-- so we can exceed it slightly if max_threads > 1, but we'll still prevent a lot of scans and part processing)
CREATE TABLE row_limits_test
(
    i UInt32,
) ENGINE = MergeTree() ORDER BY i;

INSERT INTO row_limits_test select * from numbers(30);
INSERT INTO row_limits_test select * from numbers(30,30);

SET max_rows_to_read = 20;
SET max_threads = 1;
SET read_overflow_mode = 'throw';

SELECT /* test 01132_max_rows_to_read */ sum(i) from row_limits_test group by i order by i; -- { serverError 158 }
SYSTEM FLUSH LOGS;

SET max_rows_to_read = 0;
-- would be great to select read_rows from system.query_log, but values are 0 for queries in ExceptionWhilstProcessing state at the moment
SELECT
    toFloat32(extract(exception, 'max rows: ([0-9]+[.]?[0-9]+)')) AS max_rows,
    ceil(toFloat32(extract(exception, 'current rows: ([0-9]+[.]?[0-9]+)'))/10)*10 AS current_rows -- can be imprecise as reading can stop at any time
FROM system.query_log WHERE event_date >= now() - toIntervalDay(1) AND current_database = currentDatabase() AND exception_code = 158 AND query LIKE '%01132_max_rows_to_read%';
