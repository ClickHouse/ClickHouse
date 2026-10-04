-- A `Set` table cannot be read from, but it knows exactly how many rows it holds, so `count` is
-- answered from that instead of failing. `Join` already behaves this way.

-- The setting randomizer in CI can turn `optimize_trivial_count_query` off, and without the
-- optimization the query falls back to reading from the storage, which a `Set` table does not
-- support. Pin it, because this test is about the optimization itself.
SET optimize_trivial_count_query = 1;

DROP TABLE IF EXISTS t_set;
CREATE TABLE t_set (key UInt64) ENGINE = Set();

SELECT 'empty';
SELECT count() FROM t_set;

INSERT INTO t_set VALUES (1), (2), (3);

SELECT 'filled';
SELECT count() FROM t_set;

SELECT 'a set stores distinct rows, so duplicates do not add to the count';
INSERT INTO t_set VALUES (1), (2);
SELECT count() FROM t_set;

SELECT 'and agrees with system.tables';
SELECT total_rows FROM system.tables WHERE database = currentDatabase() AND name = 't_set';

SELECT 'the count is usable in a subquery with an outer filter';
-- This is the query from the issue. It needs the analyzer: the old one does not push the trivial
-- count through the subquery and still asks the storage to read.
SELECT cnt FROM (SELECT count() AS cnt FROM t_set) WHERE cnt = 0 SETTINGS enable_analyzer = 1;
SELECT cnt FROM (SELECT count() AS cnt FROM t_set) WHERE cnt = 3 SETTINGS enable_analyzer = 1;

SELECT 'truncate empties it';
TRUNCATE TABLE t_set;
SELECT count() FROM t_set;

SELECT 'reading a column is still not supported';
SELECT key FROM t_set; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM t_set; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_set;
