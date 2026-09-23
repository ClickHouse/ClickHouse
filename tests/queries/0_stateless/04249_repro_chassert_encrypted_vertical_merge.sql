-- Tags: no-fasttest, no-parallel
-- Reproduces chassert(header_rope.totalBytes() == data_start_offset) in
-- ReaderExecutor::initDecryption (src/IO/ReaderExecutor.cpp:179).
--
-- Stress test of PR 103706 (SHA 9f80c9cd48a) showed background vertical
-- merges aborting on encrypted disks. Root cause: when a column substream
-- file is missing on the underlying object storage, DiskObjectStorage's
-- prepareRead installs an empty-file fallback (StoredObject with bytes_size=0
-- + ReadBufferFromEmptyFile). The wrapping DiskEncrypted still appends a
-- needDecryption stage, so ReaderExecutor::initDecryption is asked to read a
-- 64-byte encryption header from a zero-byte source and the rope comes back
-- empty (totalBytes=0 != data_start_offset=64).
--
-- Trigger: a Wide-format part on an encrypted disk that has a sparse column
-- with every value defaulted (no `.sparse.idx.bin` written), merged
-- vertically with another part that has the same column populated.

DROP TABLE IF EXISTS t_chassert_encrypted_vmerge;

CREATE TABLE t_chassert_encrypted_vmerge
(k UInt64, dense String, sparse_col String)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    storage_policy = 's3_cache_encrypted',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    -- Pin full (non-packed) part storage: a randomized min_bytes_for_full_part_storage
    -- pushes these tiny parts into packed storage, which merges horizontally and
    -- defeats the vertical-merge repro below.
    min_bytes_for_full_part_storage = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    enable_vertical_merge_algorithm = 1,
    ratio_of_defaults_for_sparse_serialization = 0.95;

-- Part 1: sparse_col is 100% default ('') — should be stored as Sparse
-- with no .sparse.idx.bin (no non-default positions to index).
INSERT INTO t_chassert_encrypted_vmerge
SELECT number, randomPrintableASCII(50), ''
FROM numbers(10000);

-- Part 2: sparse_col is 100% non-default — stored as Default (dense).
INSERT INTO t_chassert_encrypted_vmerge
SELECT number + 10000, randomPrintableASCII(50), randomPrintableASCII(50)
FROM numbers(10000);

-- Part 3: sparse_col is 100% default again — Sparse, no .sparse.idx.bin.
INSERT INTO t_chassert_encrypted_vmerge
SELECT number + 20000, randomPrintableASCII(50), ''
FROM numbers(10000);

OPTIMIZE TABLE t_chassert_encrypted_vmerge FINAL;

SYSTEM FLUSH LOGS part_log;
SELECT merge_algorithm
FROM system.part_log
WHERE database = currentDatabase()
  AND table = 't_chassert_encrypted_vmerge'
  AND event_type = 'MergeParts'
ORDER BY event_time
LIMIT 1;

SELECT count(), countIf(sparse_col != '') FROM t_chassert_encrypted_vmerge;

DROP TABLE t_chassert_encrypted_vmerge;
