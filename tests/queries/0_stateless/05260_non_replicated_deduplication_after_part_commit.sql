-- Tags: no-replicated-database
-- no-replicated-database: as in `05229_max_table_size_rows_multiple_parts`, the test asserts how much of a partially
-- rejected insert survives, and with the Replicated database engine the parts are committed through the
-- ReplicatedMergeTree sink instead.

-- The block IDs of a part must be recorded in the deduplication log of a non-replicated MergeTree only after the part
-- is committed. Here the second part of a multi-partition insert is rejected by `max_table_size_rows` after the first
-- one was committed. A retry must insert the rejected part and deduplicate the committed one.
-- https://github.com/ClickHouse/ClickHouse/issues/122275

SET async_insert = 0;
SET deduplicate_insert = 'enable';

DROP TABLE IF EXISTS t_dedup_after_part_commit;

CREATE TABLE t_dedup_after_part_commit (p UInt8, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x
    SETTINGS non_replicated_deduplication_window = 100, max_table_size_rows = 3;

INSERT INTO t_dedup_after_part_commit VALUES (0, 1), (0, 2), (0, 3), (0, 4), (0, 5), (1, 1), (1, 2), (1, 3), (1, 4), (1, 5); -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }
SELECT 'after the rejected insert', count() FROM t_dedup_after_part_commit;

-- Reload the deduplication log from the disk, so that a record of the rejected part on the disk would be found as well.
DETACH TABLE t_dedup_after_part_commit;
ATTACH TABLE t_dedup_after_part_commit;

ALTER TABLE t_dedup_after_part_commit MODIFY SETTING max_table_size_rows = 0;

INSERT INTO t_dedup_after_part_commit VALUES (0, 1), (0, 2), (0, 3), (0, 4), (0, 5), (1, 1), (1, 2), (1, 3), (1, 4), (1, 5);
SELECT 'after the retry', p, arraySort(groupArray(x)) FROM t_dedup_after_part_commit GROUP BY p ORDER BY p;

-- Both parts are committed now, so another retry is deduplicated as a whole.
INSERT INTO t_dedup_after_part_commit VALUES (0, 1), (0, 2), (0, 3), (0, 4), (0, 5), (1, 1), (1, 2), (1, 3), (1, 4), (1, 5);
SELECT 'after the second retry', count() FROM t_dedup_after_part_commit;
SELECT 'active parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_dedup_after_part_commit' AND active;

DROP TABLE t_dedup_after_part_commit;
