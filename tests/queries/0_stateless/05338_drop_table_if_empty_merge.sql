-- Dropping or detaching a `Merge` table with `IF EMPTY` removes only the wrapper, which holds no rows
-- of its own, so it is not judged by the row count of its source tables, even when a source table
-- does not know its row count.

DROP TABLE IF EXISTS t_merge_if_empty_mt;
DROP TABLE IF EXISTS t_merge_if_empty_file;
DROP TABLE IF EXISTS m_merge_if_empty;

CREATE TABLE t_merge_if_empty_mt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_merge_if_empty_mt VALUES (1), (2), (3);
CREATE TABLE t_merge_if_empty_file (x UInt64) ENGINE = File(TSV);
INSERT INTO t_merge_if_empty_file VALUES (1);

CREATE TABLE m_merge_if_empty (x UInt64) ENGINE = Merge(currentDatabase(), '^t_merge_if_empty_');
SELECT count() FROM m_merge_if_empty;

DETACH TABLE IF EMPTY m_merge_if_empty;
ATTACH TABLE m_merge_if_empty;
DROP TABLE IF EMPTY m_merge_if_empty SETTINGS ignore_drop_queries_probability = 0;

-- The source table itself does not know its row count, so `IF EMPTY` refuses to drop it.
DROP TABLE IF EMPTY t_merge_if_empty_file SETTINGS ignore_drop_queries_probability = 0; -- { serverError TABLE_NOT_EMPTY }

SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '%merge_if_empty%' ORDER BY name;
SELECT count() FROM t_merge_if_empty_mt;
SELECT count() FROM t_merge_if_empty_file;

DROP TABLE t_merge_if_empty_mt;
DROP TABLE t_merge_if_empty_file;
