DROP ROW POLICY IF EXISTS test_filter_policy ON test_table;
DROP ROW POLICY IF EXISTS test_filter_policy_2 ON test_table;
DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table (`n` UInt64, `s` String)
ENGINE = MergeTree
PRIMARY KEY n ORDER BY n;

INSERT INTO test_table SELECT number, concat('some string ', CAST(number, 'String')) FROM numbers(1000000);

-- Create row policy that doesn't use any column
CREATE ROW POLICY test_filter_policy ON test_table USING False TO ALL;

-- Run query under default user so that always false row_level_filter is added that doesn't require any columns
SELECT count(1) FROM test_table;
SELECT count(1) FROM test_table PREWHERE (n % 8192) < 4000;
SELECT count(1) FROM test_table WHERE (n % 8192) < 4000;
SELECT count(1) FROM test_table PREWHERE (n % 8192) < 4000 WHERE (n % 33) == 0;

-- Add policy for default user that will read a column
CREATE ROW POLICY test_filter_policy_2 ON test_table USING (n % 5) >= 3 TO default;

-- Run query under default user that needs the same column as PREWHERE and WHERE
SELECT count(1) FROM test_table;
SELECT count(1) FROM test_table PREWHERE (n % 8192) < 4000;
SELECT count(1) FROM test_table WHERE (n % 8192) < 4000;
SELECT count(1) FROM test_table PREWHERE (n % 8192) < 4000 WHERE (n % 33) == 0;

-- Run queries that have division by zero if row level filter isn't applied before prewhere
SELECT count(1) FROM test_table PREWHERE 7 / (n % 5) > 2;
SELECT count(1) FROM test_table WHERE 7 / (n % 5) > 2;
SELECT count(1) FROM test_table PREWHERE 7 / (n % 5) > 2 WHERE (n % 33) == 0;

DROP TABLE test_table;
DROP ROW POLICY test_filter_policy ON test_table;
DROP ROW POLICY test_filter_policy_2 ON test_table;
