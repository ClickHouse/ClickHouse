-- Tags: no-fasttest
-- no-fasttest: needs an object-storage disk (minio) with a non-fake transaction.

-- Regression test for packed skip-index size accounting after DROP INDEX on object storage.
--
-- The s3_no_fake_transaction policy uses use_fake_transaction=false (as ClickHouse Cloud /
-- Keeper metadata does), so file writes are batched into the part transaction and the rewritten
-- skp_idx.packed isn't visible on disk until that transaction commits. With
-- columns_and_secondary_indices_sizes_lazy_calculation=0 the per-part secondary-index size
-- accounting runs inside finalizeMutatedPart, before that commit. The DROP INDEX drop-only path
-- (no writer pipeline) rebuilds the archive directly; if it doesn't seed the new storage's reader
-- from the in-memory archive index, the accounting probes the not-yet-committed archive, sees
-- nothing, and latches the surviving packed index's size at zero.

DROP TABLE IF EXISTS packed_drop_size SYNC;

CREATE TABLE packed_drop_size
(
    id UInt64,
    v UInt64,
    w UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1,
    INDEX m_w w TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 's3_no_fake_transaction',
         min_bytes_for_wide_part = 0,
         packed_skip_index_max_bytes = '1M',
         index_granularity = 1024,
         columns_and_secondary_indices_sizes_lazy_calculation = 0;

INSERT INTO packed_drop_size SELECT number, number * 7, number * 11 FROM numbers(2000);

ALTER TABLE packed_drop_size DROP INDEX m_v SETTINGS mutations_sync = 2, alter_sync = 2;

-- The surviving packed index must still report a nonzero on-disk size (it was 0 before the fix).
SELECT name, data_compressed_bytes > 0
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'packed_drop_size'
ORDER BY name;

DROP TABLE packed_drop_size SYNC;

-- Sibling case: a mutation that leaves the archive unchanged hardlinks skp_idx.packed into the
-- new part instead of rewriting it. The hardlink is just as invisible during pre-commit size
-- accounting, so the new storage must be seeded from the source archive index there too.

DROP TABLE IF EXISTS packed_hardlink_size SYNC;

CREATE TABLE packed_hardlink_size
(
    id UInt64,
    v UInt64,
    s String,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 's3_no_fake_transaction',
         min_bytes_for_wide_part = 0,
         packed_skip_index_max_bytes = '1M',
         index_granularity = 1024,
         columns_and_secondary_indices_sizes_lazy_calculation = 0;

INSERT INTO packed_hardlink_size SELECT number, number * 7, toString(number % 50) FROM numbers(2000);

-- UPDATE a non-indexed column: the minmax archive is unchanged and gets hardlinked.
ALTER TABLE packed_hardlink_size UPDATE s = concat(s, '_x') WHERE id < 100 SETTINGS mutations_sync = 2, alter_sync = 2;

SELECT name, data_compressed_bytes > 0
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'packed_hardlink_size'
ORDER BY name;

DROP TABLE packed_hardlink_size SYNC;
