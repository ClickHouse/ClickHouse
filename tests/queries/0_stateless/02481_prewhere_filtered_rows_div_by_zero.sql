DROP TABLE IF EXISTS test_filter;

-- { echoOn }
CREATE TABLE test_filter(a Int32, b Int32, c Int32) ENGINE = MergeTree() ORDER BY a SETTINGS index_granularity = 3, index_granularity_bytes = '10Mi';

INSERT INTO test_filter SELECT number, number+1, (number/2 + 1) % 2 FROM numbers(15);

SELECT _part_offset, intDiv(_part_offset, 3) as granule, * FROM test_filter ORDER BY _part_offset;

-- Check that division by zero occurs on some rows
SELECT intDiv(b, c) FROM test_filter;  -- { serverError ILLEGAL_DIVISION }
-- Filter out those rows using WHERE or PREWHERE
SELECT intDiv(b, c) FROM test_filter WHERE c != 0;
SELECT intDiv(b, c) FROM test_filter PREWHERE c != 0;
SELECT intDiv(b, c) FROM test_filter PREWHERE c != 0 WHERE b%2 != 0;


SET mutations_sync = 2;

-- Delete all rows where division by zero could occur
DELETE FROM test_filter WHERE c = 0;
-- Test that now division by zero doesn't occur without explicit condition
SELECT intDiv(b, c) FROM test_filter;
SELECT * FROM test_filter PREWHERE intDiv(b, c) > 0;
SELECT * FROM test_filter PREWHERE b != 0 WHERE intDiv(b, c) > 0;

-- { echoOff }
DROP TABLE test_filter;
