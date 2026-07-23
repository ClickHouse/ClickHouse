-- Tags: no-fasttest, no-parallel

-- Test for buffer pointer invalidation in ReadBufferFromEncryptedFile when
-- async prefetch is enabled with cached encrypted S3 storage.
-- AsynchronousBoundedReadBuffer swaps internal_buffer/working_buffer during
-- prefetch, which can interact unsafely with the encrypted reader's decrypt
-- that writes into working_buffer.

SET remote_filesystem_read_prefetch = 1;
SET remote_filesystem_read_method = 'threadpool';

DROP TABLE IF EXISTS t_encrypted_s3_cache_prefetch;

CREATE TABLE t_encrypted_s3_cache_prefetch (key UInt64, a String, b String, c String)
ENGINE = MergeTree ORDER BY key
SETTINGS storage_policy = 's3_cache_encrypted', min_bytes_for_wide_part = 0;

-- Insert enough data to span multiple file segments and trigger prefetch.
INSERT INTO t_encrypted_s3_cache_prefetch
SELECT number, randomPrintableASCII(200), randomPrintableASCII(200), randomPrintableASCII(200)
FROM numbers(100000);

SYSTEM DROP FILESYSTEM CACHE 's3_cache';

-- Read with multiple threads to exercise parallel prefetch on encrypted data.
SELECT count() FROM t_encrypted_s3_cache_prefetch WHERE NOT ignore(a, b, c);

-- Read with prewhere to exercise separate column reads through the encrypted buffer chain.
SYSTEM DROP FILESYSTEM CACHE 's3_cache';
SELECT count() FROM t_encrypted_s3_cache_prefetch PREWHERE key % 10 = 0 WHERE NOT ignore(a, b, c);

DROP TABLE t_encrypted_s3_cache_prefetch;
