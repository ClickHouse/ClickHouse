-- Tags: no-fasttest

-- Verify that s3_read_request_duration_microseconds and s3_read_request_bytes histogram metrics
-- are populated after reading data from S3.

DROP TABLE IF EXISTS test_s3_metrics;
CREATE TABLE test_s3_metrics (key UInt64, value String)
ENGINE = MergeTree ORDER BY key
SETTINGS storage_policy = 's3_no_cache';

INSERT INTO test_s3_metrics SELECT number, repeat('x', 1000) FROM numbers(10000);

-- Force a read from S3.
SELECT count() FROM test_s3_metrics WHERE NOT ignore(value) FORMAT Null;

-- Check that duration metric has observations (+Inf bucket is cumulative total count).
SELECT value > 0
FROM system.histogram_metrics
WHERE name = 's3_read_request_duration_microseconds'
  AND labels['le'] = '+Inf';

-- Check that bytes metric has observations.
SELECT value > 0
FROM system.histogram_metrics
WHERE name = 's3_read_request_bytes'
  AND labels['le'] = '+Inf';

DROP TABLE test_s3_metrics;
