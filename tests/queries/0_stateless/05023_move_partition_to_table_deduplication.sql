-- `MOVE PARTITION TO TABLE` retires the moved-out partition in the source table, so the block ids
-- of the moved data must be retired in the source deduplication log as well. Otherwise a retry of
-- an insert of that data into the source table is silently deduplicated against a partition the
-- source no longer contains, and the rows are lost.

DROP TABLE IF EXISTS t_move_dedup_src;
DROP TABLE IF EXISTS t_move_dedup_dst;

CREATE TABLE t_move_dedup_src (x UInt64) ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
    SETTINGS non_replicated_deduplication_window = 100;
CREATE TABLE t_move_dedup_dst (x UInt64) ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
    SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO t_move_dedup_src VALUES (1);
INSERT INTO t_move_dedup_src VALUES (2);

-- The same block is deduplicated while the data is still there.
INSERT INTO t_move_dedup_src VALUES (1);
SELECT 'after inserts', count() FROM t_move_dedup_src;

ALTER TABLE t_move_dedup_src MOVE PARTITION 1 TO TABLE t_move_dedup_dst;
SELECT 'after move, src', count() FROM t_move_dedup_src;
SELECT 'after move, dst', count() FROM t_move_dedup_dst;

-- The moved-out data is not in the source table any more, so re-inserting it must not be
-- deduplicated away.
INSERT INTO t_move_dedup_src VALUES (1);
SELECT 'after reinsert, src', count() FROM t_move_dedup_src;
SELECT 'after reinsert, x', x FROM t_move_dedup_src ORDER BY x;

-- Deduplication still works for the re-inserted block.
INSERT INTO t_move_dedup_src VALUES (1);
SELECT 'after duplicate, src', count() FROM t_move_dedup_src;

DROP TABLE t_move_dedup_src;
DROP TABLE t_move_dedup_dst;
