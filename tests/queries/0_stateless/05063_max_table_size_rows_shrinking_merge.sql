-- https://github.com/ClickHouse/ClickHouse/issues/117101
-- A merge that does not increase the row count has to be allowed even when the table already
-- exceeds `max_table_size_rows`, otherwise an over-limit table can never be compacted: every
-- partial merge throws and the part count grows until inserts start failing. The byte limits
-- already have that carve-out.

DROP TABLE IF EXISTS t_max_table_size_rows;
-- Prevent regular background merges (by the max size of merged parts) so that the small parts of
-- partition 1 survive until `OPTIMIZE`, which ignores this setting, merges them - otherwise the test
-- could pass on the background merge path without ever exercising the over-limit merge.
CREATE TABLE t_max_table_size_rows (p UInt8, id UInt64, s String) ENGINE = MergeTree PARTITION BY p ORDER BY id
    SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;

-- partition 0: one big part; partition 1: five small parts
INSERT INTO t_max_table_size_rows SELECT 0, number, 'x' FROM numbers(5000);
INSERT INTO t_max_table_size_rows SELECT 1, number, 'a' FROM numbers(20);
INSERT INTO t_max_table_size_rows SELECT 1, number + 20, 'b' FROM numbers(20);
INSERT INTO t_max_table_size_rows SELECT 1, number + 40, 'c' FROM numbers(20);
INSERT INTO t_max_table_size_rows SELECT 1, number + 60, 'd' FROM numbers(20);
INSERT INTO t_max_table_size_rows SELECT 1, number + 80, 'e' FROM numbers(20);

ALTER TABLE t_max_table_size_rows MODIFY SETTING max_table_size_rows = 1000;

-- All five parts of partition 1 are still there, so `OPTIMIZE` below really has to merge them.
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_max_table_size_rows' AND active AND partition = '1';

-- Compacting partition 1 keeps the row count, so it is allowed.
OPTIMIZE TABLE t_max_table_size_rows PARTITION 1 FINAL;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_max_table_size_rows' AND active AND partition = '1';
SELECT count() FROM t_max_table_size_rows;

-- An insert into the over-limit table is still rejected.
INSERT INTO t_max_table_size_rows SELECT 1, number + 1000, 'f' FROM numbers(20); -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }

SET mutations_sync = 2;

-- A mutation that deletes only some of the rows is allowed as well, even though the table is still
-- far above the limit afterwards: the mutated part holds fewer rows than the part it replaces.
ALTER TABLE t_max_table_size_rows DELETE WHERE p = 0 AND id < 2000;
SELECT count() FROM t_max_table_size_rows;

-- A mutation that deletes the rest brings the table back under the limit.
ALTER TABLE t_max_table_size_rows DELETE WHERE p = 0;
SELECT count() FROM t_max_table_size_rows;

-- And once under the limit, inserts work again.
INSERT INTO t_max_table_size_rows SELECT 1, number + 1000, 'f' FROM numbers(20);
SELECT count() FROM t_max_table_size_rows;

DROP TABLE t_max_table_size_rows;
