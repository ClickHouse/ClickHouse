-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/84707
-- ORC library requires rangeSizeLimit > holeSizeLimit.
-- When remote_read_min_bytes_for_seek >= 10MB (the hardcoded rangeSizeLimit),
-- the assertion in coalesceReadRanges was violated.

SET s3_truncate_on_insert = 1;

INSERT INTO FUNCTION s3(s3_conn, filename='test_04050_orc.orc', format='ORC')
SELECT number AS a, toString(number) AS b FROM numbers(100);

-- 10MB, equal to the old hardcoded rangeSizeLimit
SELECT count() FROM s3(s3_conn, filename='test_04050_orc.orc', format='ORC')
SETTINGS remote_read_min_bytes_for_seek = 10485760;

-- Larger than 10MB
SELECT count() FROM s3(s3_conn, filename='test_04050_orc.orc', format='ORC')
SETTINGS remote_read_min_bytes_for_seek = 104857600;

-- Max UInt64 value (overflow edge case)
SELECT count() FROM s3(s3_conn, filename='test_04050_orc.orc', format='ORC')
SETTINGS remote_read_min_bytes_for_seek = 18446744073709551615;
