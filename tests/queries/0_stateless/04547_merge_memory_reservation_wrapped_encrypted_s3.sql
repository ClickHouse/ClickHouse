-- Tags: no-fasttest
-- Tag no-fasttest: requires S3 (minio)

-- Regression test for the merge memory reservation on a decorator (wrapped) object-storage disk
-- (see CompactionStatistics::getDiskWriteBufferMemory). An encrypted disk delegates writes to the
-- disk it wraps, so a merge onto encrypted-over-S3 allocates the same multipart upload buffers as onto bare
-- S3, and the reservation must take its per-stream ceiling from the wrapped disk's request settings instead
-- of falling back to the query/session settings (which a background object-storage writer ignores). The
-- s3_no_cache_encrypted_double policy stacks two encryption layers, so the ceiling lookup has to unwrap
-- multiple decorator levels down to the real S3 disk. The explicit OPTIMIZE goes through the
-- reservation-and-correction path of StorageMergeTree::selectPartsToMerge, where the destination disk
-- resolved by the tagger is the wrapped (encrypted) disk. The large merge_selecting_sleep_ms keeps
-- background merge selection from firing before the explicit OPTIMIZE below.

DROP TABLE IF EXISTS t_merge_mem_wrapped_disk;

CREATE TABLE t_merge_mem_wrapped_disk (k UInt64, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS storage_policy = 's3_no_cache_encrypted_double', min_bytes_for_wide_part = 0,
    merge_selecting_sleep_ms = 600000, max_merge_selecting_sleep_ms = 600000;

INSERT INTO t_merge_mem_wrapped_disk SELECT number, toString(number) FROM numbers(1000);
INSERT INTO t_merge_mem_wrapped_disk SELECT number, toString(number) FROM numbers(1000, 1000);
INSERT INTO t_merge_mem_wrapped_disk SELECT number, toString(number) FROM numbers(2000, 1000);

OPTIMIZE TABLE t_merge_mem_wrapped_disk FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT count(), sum(k) FROM t_merge_mem_wrapped_disk;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_wrapped_disk' AND active;

DROP TABLE t_merge_mem_wrapped_disk;
