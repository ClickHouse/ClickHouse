-- Tags: no-fasttest, no-parallel

-- Smoke test for encrypted disk over cached S3 storage.
-- Verifies the `s3_cache_encrypted` disk and storage policy are configured and functional.

DROP TABLE IF EXISTS t_encrypted_s3_cache_smoke;

CREATE TABLE t_encrypted_s3_cache_smoke (x UInt64, s String)
ENGINE = MergeTree ORDER BY x
SETTINGS storage_policy = 's3_cache_encrypted';

INSERT INTO t_encrypted_s3_cache_smoke SELECT number, toString(number) FROM numbers(1000);
SELECT count(), sum(x) FROM t_encrypted_s3_cache_smoke;

-- Verify data survives a cache drop and re-read from S3.
SYSTEM DROP FILESYSTEM CACHE 's3_cache';
SELECT count(), sum(x) FROM t_encrypted_s3_cache_smoke;

DROP TABLE t_encrypted_s3_cache_smoke;
