-- Tags: no-fasttest
-- no-fasttest: needs an object-storage disk (minio). Fast test passes --fast-test to
-- tests/config/install.sh, which clears EXPORT_S3_STORAGE_POLICIES, so the s3 policies are not
-- installed there at all.

-- MergeTreeData::Transaction::commit updates the table's per-column and per-secondary-index size
-- aggregates inside a NOEXCEPT_SCOPE. For a part whose sizes are not computed yet, that update used
-- to call the size getters, which lazily read the part storage. On packed storage that read resolves
-- the skip-index archive index, and on object storage it is scheduled onto a thread pool. A thread
-- pool that cannot schedule throws CANNOT_SCHEDULE_TASK, which is not a memory-tracker exception, so
-- LockMemoryExceptionInThread does not suppress it, it reaches the NOEXCEPT_SCOPE catch-all, and the
-- server terminates.
--
-- The commit path must therefore perform no storage read at all. This test measures that through
-- the MutatePart row of system.part_log: ProfileEventsScope in MutatePlainMergeTreeTask::executeStep
-- covers transaction.commit, and write_part_log runs after it on the same thread, so a read taken
-- inside the commit is counted there.
--
-- The cold arm is compared against a warm control rather than against an absolute count: with
-- lazy_calculation = 0 the part is warmed when it is loaded, so that arm always contains exactly one
-- archive read and never needs another one in the commit. Absolute counts would depend on the
-- fixture, the control makes the assertion self-normalizing.

DROP TABLE IF EXISTS packed_commit_cold SYNC;
DROP TABLE IF EXISTS packed_commit_warm SYNC;

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

-- String columns on purpose: loadRowsCount computes on-disk sizes for numeric columns in debug and
-- sanitizer builds, which would warm the cloned part and hide the read.
INSERT INTO packed_commit_cold SELECT toString(number), toString(number * 7), toString(number * 11) FROM numbers(2000);
INSERT INTO packed_commit_warm SELECT toString(number), toString(number * 7), toString(number * 11) FROM numbers(2000);

-- Both parts must be Packed, otherwise there is no archive to read and the test is vacuous.
SELECT 'packed', countDistinct(part_storage_type) = 1, any(part_storage_type)
FROM system.parts
WHERE database = currentDatabase() AND table LIKE 'packed_commit_%' AND active;

-- Reading a size sets the table level flag, which is what makes commit update the aggregates at all.
SELECT 'accounted', sum(data_compressed_bytes) > 0
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table LIKE 'packed_commit_%';

-- A predicate matching no rows takes the untouched-part shortcut, which clones the part and keeps
-- its block range, so the source part is covered and both accounting paths run in the commit.
ALTER TABLE packed_commit_cold UPDATE a = concat(a, 'x') WHERE s = 'no_such_key' SETTINGS mutations_sync = 2;
ALTER TABLE packed_commit_warm UPDATE a = concat(a, 'x') WHERE s = 'no_such_key' SETTINGS mutations_sync = 2;

SELECT 'untouched', value > 0 FROM system.events WHERE event = 'MutationUntouchedParts';

SYSTEM FLUSH LOGS part_log;

SELECT 'rows', count() = 2
FROM system.part_log
WHERE database = currentDatabase() AND table LIKE 'packed_commit_%' AND event_type = 'MutatePart';

-- The warm control computes the sizes when it loads the cloned part, so its MutatePart row always
-- contains that one archive read. The cold arm must contain strictly fewer file opens, because
-- after the fix it does not read the archive at all: not when loading (the sizes stay lazy) and not
-- in the commit (the aggregates are dropped instead of updated). Before the fix it read in the
-- commit, so it opened the same number of files as the control.
--
-- Strictly fewer, not "no more than": the unfixed build reads exactly as much as the control, so a
-- "no more than" assertion would hold on both builds and the test would prove nothing.
--
-- FileOpen is the counter because it does not depend on which reader implementation serves the
-- read, so the assertion survives local_filesystem_read_method and remote_filesystem_read_method
-- randomization.
SELECT 'fewer_file_open',
    maxIf(ProfileEvents['FileOpen'], table = 'packed_commit_cold')
   < maxIf(ProfileEvents['FileOpen'], table = 'packed_commit_warm')
FROM system.part_log
WHERE database = currentDatabase() AND table LIKE 'packed_commit_%' AND event_type = 'MutatePart';

-- The same statement in bytes read through the asynchronous reader. This is the counter that ties
-- the test to the abort: on object storage the archive read is handed to a thread pool, and a pool
-- that cannot schedule is where CANNOT_SCHEDULE_TASK comes from.
SELECT 'fewer_pool_read',
    maxIf(ProfileEvents['ThreadpoolReaderReadBytes'], table = 'packed_commit_cold')
   < maxIf(ProfileEvents['ThreadpoolReaderReadBytes'], table = 'packed_commit_warm')
FROM system.part_log
WHERE database = currentDatabase() AND table LIKE 'packed_commit_%' AND event_type = 'MutatePart';

-- The aggregates must stay correct: dropping them and rebuilding on the next read has to give the
-- same answer the incremental update gave.
SELECT 'sizes_correct', name, data_compressed_bytes > 0
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'packed_commit_cold'
ORDER BY name;

SELECT 'rows_intact', count() FROM packed_commit_cold;

DROP TABLE packed_commit_cold SYNC;
DROP TABLE packed_commit_warm SYNC;
