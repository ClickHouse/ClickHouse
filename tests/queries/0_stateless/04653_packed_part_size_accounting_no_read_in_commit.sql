-- Tags: no-fasttest
-- no-fasttest: needs an object-storage disk (minio). Fast test passes --fast-test to
-- tests/config/install.sh, which clears EXPORT_S3_STORAGE_POLICIES, so the s3 policies are not
-- installed there at all.

-- The object-storage policy is kept because that is the configuration the aborting read was observed
-- in; the counter itself is disk-independent.

-- `MergeTreeData::Transaction::commit` updates the table's per-column and per-secondary-index size
-- aggregates inside a `NOEXCEPT_SCOPE` whose catch-all terminates the server. For a part whose sizes
-- are not computed yet, that update called the size getters, which lazily read the part storage, and
-- when the skip indices are bundled into a packed archive that read resolves the archive's index.
-- Under the default `local_filesystem_read_method = 'pread_threadpool'` it is submitted to a thread
-- pool, which is where the reproduced abort came from
-- (`AsynchronousReadBufferFromFileDescriptor::asyncReadInto` -> `ThreadPoolImpl::scheduleImpl`): a
-- pool that cannot schedule throws `CANNOT_SCHEDULE_TASK`, not a memory-tracker exception, so
-- `LockMemoryExceptionInThread` does not suppress it and it escapes.
--
-- The commit must therefore read nothing. `PackedSkipIndicesArchiveIndexLoads` counts that read, and
-- is read per table from the `MutatePart` row of `system.part_log`: `ProfileEventsScope` in
-- `MutatePlainMergeTreeTask::executeStep` covers `transaction.commit`, and `write_part_log` runs after
-- it on the same thread, so the counters it collects are the mutation's own.

DROP TABLE IF EXISTS packed_commit_cold SYNC;
DROP TABLE IF EXISTS packed_commit_warm SYNC;
DROP TABLE IF EXISTS full_commit_archive SYNC;
DROP TABLE IF EXISTS full_commit_no_archive SYNC;

-- Cold arm: the mutation's cloned part arrives with its sizes not yet computed.
CREATE TABLE packed_commit_cold
(
    s String,
    a String,
    b String,
    INDEX m_a a TYPE minmax GRANULARITY 1,
    INDEX m_b b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY s
SETTINGS storage_policy = 's3_no_fake_transaction',
         min_bytes_for_wide_part = 0,
         packed_skip_index_max_bytes = '1M',
         index_granularity = 1024,
         min_bytes_for_full_part_storage = 1000000000,
         columns_and_secondary_indices_sizes_lazy_calculation = 1;

-- Warm control: identical except that the sizes are computed when the part is loaded, so the
-- commit has no reason to read even before the fix.
CREATE TABLE packed_commit_warm
(
    s String,
    a String,
    b String,
    INDEX m_a a TYPE minmax GRANULARITY 1,
    INDEX m_b b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY s
SETTINGS storage_policy = 's3_no_fake_transaction',
         min_bytes_for_wide_part = 0,
         packed_skip_index_max_bytes = '1M',
         index_granularity = 1024,
         min_bytes_for_full_part_storage = 1000000000,
         columns_and_secondary_indices_sizes_lazy_calculation = 0;

-- `Full` part storage keeps the packed skip-indices archive as a standalone file, which a different
-- reader opens, so it is covered by its own arm. The second table writes the same two indices with
-- packing disabled, so a 0 there means "no archive to load" rather than "no indices".
CREATE TABLE full_commit_archive
(
    s String,
    a String,
    b String,
    INDEX m_a a TYPE minmax GRANULARITY 1,
    INDEX m_b b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY s
SETTINGS storage_policy = 's3_no_fake_transaction',
         min_bytes_for_wide_part = 0,
         packed_skip_index_max_bytes = '1M',
         index_granularity = 1024,
         min_bytes_for_full_part_storage = 0,
         columns_and_secondary_indices_sizes_lazy_calculation = 0;

CREATE TABLE full_commit_no_archive
(
    s String,
    a String,
    b String,
    INDEX m_a a TYPE minmax GRANULARITY 1,
    INDEX m_b b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY s
SETTINGS storage_policy = 's3_no_fake_transaction',
         min_bytes_for_wide_part = 0,
         packed_skip_index_max_bytes = 0,
         index_granularity = 1024,
         min_bytes_for_full_part_storage = 0,
         columns_and_secondary_indices_sizes_lazy_calculation = 0;

-- String columns on purpose: `loadRowsCount` computes on-disk sizes for numeric columns in debug and
-- sanitizer builds, which would warm the cloned part and hide the read.
INSERT INTO packed_commit_cold SELECT toString(number), toString(number * 7), toString(number * 11) FROM numbers(2000);
INSERT INTO packed_commit_warm SELECT toString(number), toString(number * 7), toString(number * 11) FROM numbers(2000);
INSERT INTO full_commit_archive SELECT toString(number), toString(number * 7), toString(number * 11) FROM numbers(2000);
INSERT INTO full_commit_no_archive SELECT toString(number), toString(number * 7), toString(number * 11) FROM numbers(2000);

-- Both parts must be `Packed`, otherwise there is no archive to read and the test is vacuous.
SELECT 'packed', countDistinct(part_storage_type) = 1, any(part_storage_type)
FROM system.parts
WHERE database = currentDatabase() AND table LIKE 'packed_commit_%' AND active;

-- Reading a size sets the table level flag, which is what makes the commit update the aggregates at
-- all.
SELECT 'accounted', sum(data_compressed_bytes) > 0
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table LIKE 'packed_commit_%';

-- A predicate matching no rows takes the untouched-part shortcut, which clones the part and keeps
-- its block range, so the source part is covered and both accounting paths run in the commit.
-- `untouched` below asserts that shortcut was taken.
ALTER TABLE packed_commit_cold UPDATE a = concat(a, 'x') WHERE s = 'no_such_key' SETTINGS mutations_sync = 2;
ALTER TABLE packed_commit_warm UPDATE a = concat(a, 'x') WHERE s = 'no_such_key' SETTINGS mutations_sync = 2;

SYSTEM FLUSH LOGS part_log;

SELECT 'rows', count() = 2
FROM system.part_log
WHERE database = currentDatabase() AND table LIKE 'packed_commit_%' AND event_type = 'MutatePart';

-- `MutationUntouchedParts` is read per table from the `MutatePart` row rather than from
-- `system.events`, whose counters are process-wide and lifetime-cumulative, so a concurrent test
-- taking the same shortcut would satisfy the assertion for us and it would fail open.
SELECT 'untouched',
    minIf(ProfileEvents['MutationUntouchedParts'], table = 'packed_commit_cold') > 0
  AND minIf(ProfileEvents['MutationUntouchedParts'], table = 'packed_commit_warm') > 0
FROM system.part_log
WHERE database = currentDatabase() AND table LIKE 'packed_commit_%' AND event_type = 'MutatePart';

-- The commit must not load the skip-indices archive index at all, so the cold arm's count is 0. The
-- warm control computes the sizes when it loads the cloned part, and that load is single-threaded and
-- memoized, so its count is exactly 1.
SELECT 'cold_no_archive_load',
    maxIf(ProfileEvents['PackedSkipIndicesArchiveIndexLoads'], table = 'packed_commit_cold') = 0
FROM system.part_log
WHERE database = currentDatabase() AND table LIKE 'packed_commit_%' AND event_type = 'MutatePart';

SELECT 'warm_one_archive_load',
    maxIf(ProfileEvents['PackedSkipIndicesArchiveIndexLoads'], table = 'packed_commit_warm') = 1
FROM system.part_log
WHERE database = currentDatabase() AND table LIKE 'packed_commit_%' AND event_type = 'MutatePart';

SELECT 'full_storage', countDistinct(part_storage_type) = 1, any(part_storage_type)
FROM system.parts
WHERE database = currentDatabase() AND table LIKE 'full_commit_%' AND active;

SELECT 'full_indices', count() = 2, min(idx_bytes > 0)
FROM
(
    SELECT sum(secondary_indices_compressed_bytes) AS idx_bytes
    FROM system.parts
    WHERE database = currentDatabase() AND table LIKE 'full_commit_%' AND active
    GROUP BY table
);

ALTER TABLE full_commit_archive UPDATE a = concat(a, 'x') WHERE s = 'no_such_key' SETTINGS mutations_sync = 2;
ALTER TABLE full_commit_no_archive UPDATE a = concat(a, 'x') WHERE s = 'no_such_key' SETTINGS mutations_sync = 2;

SYSTEM FLUSH LOGS part_log;

SELECT 'full_rows', count() = 2
FROM system.part_log
WHERE database = currentDatabase() AND table LIKE 'full_commit_%' AND event_type = 'MutatePart';

-- Both rows have to exist, otherwise `maxIf` over an empty set would satisfy the `= 0` row below.
SELECT 'full_untouched',
    minIf(ProfileEvents['MutationUntouchedParts'], table = 'full_commit_archive') > 0
  AND minIf(ProfileEvents['MutationUntouchedParts'], table = 'full_commit_no_archive') > 0
FROM system.part_log
WHERE database = currentDatabase() AND table LIKE 'full_commit_%' AND event_type = 'MutatePart';

SELECT 'full_one_archive_load',
    maxIf(ProfileEvents['PackedSkipIndicesArchiveIndexLoads'], table = 'full_commit_archive') = 1
FROM system.part_log
WHERE database = currentDatabase() AND table LIKE 'full_commit_%' AND event_type = 'MutatePart';

SELECT 'full_no_archive_zero_load',
    maxIf(ProfileEvents['PackedSkipIndicesArchiveIndexLoads'], table = 'full_commit_no_archive') = 0
FROM system.part_log
WHERE database = currentDatabase() AND table LIKE 'full_commit_%' AND event_type = 'MutatePart';

-- The aggregates must stay correct: dropping them and rebuilding on the next read has to give the
-- same answer the incremental update gave. The warm control is that answer, because it never takes
-- the invalidation branch, and the two tables are identical in schema and data. So the two rows below
-- compare the rebuilt aggregate against the incrementally updated one exactly, on every size field
-- the system tables expose: a "greater than zero" assertion would also pass for a double-counted or a
-- truncated aggregate. The `count` and the positivity flag keep the row from passing on an empty join
-- or on an all-zero answer.
--
-- Both aggregates the fix clears are covered: `system.data_skipping_indices` reads
-- `getSecondaryIndexSizes` and `system.columns` reads `getColumnSizes`. Reading them is also what
-- forces the cold arm's rebuild.
SELECT 'sizes_index_equal', count() = 2, min(sizes_equal), min(sizes_positive)
FROM
(
    SELECT
        (cold_compressed, cold_uncompressed, cold_marks) = (warm_compressed, warm_uncompressed, warm_marks) AS sizes_equal,
        cold_compressed > 0 AND cold_uncompressed > 0 AND cold_marks > 0 AS sizes_positive
    FROM
    (
        SELECT
            name,
            data_compressed_bytes AS cold_compressed,
            data_uncompressed_bytes AS cold_uncompressed,
            marks_bytes AS cold_marks
        FROM system.data_skipping_indices
        WHERE database = currentDatabase() AND table = 'packed_commit_cold'
    ) AS cold
    INNER JOIN
    (
        SELECT
            name,
            data_compressed_bytes AS warm_compressed,
            data_uncompressed_bytes AS warm_uncompressed,
            marks_bytes AS warm_marks
        FROM system.data_skipping_indices
        WHERE database = currentDatabase() AND table = 'packed_commit_warm'
    ) AS warm USING (name)
) AS index_sizes;

SELECT 'sizes_column_equal', count() = 3, min(sizes_equal), min(sizes_positive)
FROM
(
    SELECT
        (cold_compressed, cold_uncompressed, cold_marks) = (warm_compressed, warm_uncompressed, warm_marks) AS sizes_equal,
        cold_compressed > 0 AND cold_uncompressed > 0 AND cold_marks > 0 AS sizes_positive
    FROM
    (
        SELECT
            name,
            data_compressed_bytes AS cold_compressed,
            data_uncompressed_bytes AS cold_uncompressed,
            marks_bytes AS cold_marks
        FROM system.columns
        WHERE database = currentDatabase() AND table = 'packed_commit_cold'
    ) AS cold
    INNER JOIN
    (
        SELECT
            name,
            data_compressed_bytes AS warm_compressed,
            data_uncompressed_bytes AS warm_uncompressed,
            marks_bytes AS warm_marks
        FROM system.columns
        WHERE database = currentDatabase() AND table = 'packed_commit_warm'
    ) AS warm USING (name)
) AS column_sizes;

SELECT 'rows_intact', count() FROM packed_commit_cold;

DROP TABLE packed_commit_cold SYNC;
DROP TABLE packed_commit_warm SYNC;
DROP TABLE full_commit_archive SYNC;
DROP TABLE full_commit_no_archive SYNC;
