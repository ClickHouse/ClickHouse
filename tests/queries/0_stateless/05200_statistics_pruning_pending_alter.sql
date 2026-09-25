-- A pending `ALTER MODIFY COLUMN` is applied on the fly, so a read already returns the converted
-- values, while the part's statistics still describe the values as they were written. Statistics-based
-- part pruning used to compare the predicate against those stale statistics and drop the part, losing
-- rows silently until the mutation materialized.

DROP TABLE IF EXISTS t_05200;
CREATE TABLE t_05200 (x Float64 STATISTICS(basic), s String) ENGINE = MergeTree ORDER BY tuple();

-- Keep the alter mutation pending: mutations are executed by the merge scheduler.
SYSTEM STOP MERGES t_05200;

INSERT INTO t_05200 VALUES (10.5, 'a'), (11.5, 'b');
INSERT INTO t_05200 VALUES (100.5, 'c');

ALTER TABLE t_05200 MODIFY COLUMN x Int64 SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'the mutation is pending', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_05200' AND NOT is_done;

SELECT 'the values a read sees', groupArray(x) FROM (SELECT x FROM t_05200 ORDER BY x);

SELECT 'the first part', count() FROM t_05200 WHERE x = 10;
SELECT 'the first part, without pruning', count() FROM t_05200 WHERE x = 10 SETTINGS use_statistics_for_part_pruning = 0;
SELECT 'the second part', count() FROM t_05200 WHERE x = 100;
SELECT 'the second part, without pruning', count() FROM t_05200 WHERE x = 100 SETTINGS use_statistics_for_part_pruning = 0;
SELECT 'a range', count() FROM t_05200 WHERE x BETWEEN 10 AND 12;
SELECT 'a value that is really absent', count() FROM t_05200 WHERE x = 42;

-- Let the mutation materialize. Mutations are applied in order of their version, so waiting for a
-- later one means the alter is done too.
SYSTEM START MERGES t_05200;
ALTER TABLE t_05200 UPDATE s = s WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'after the mutation materialized', count() FROM t_05200 WHERE x = 10;
SELECT 'the statistics prune again', count() FROM t_05200 WHERE x = 42;

DROP TABLE t_05200;

-- A pending `DROP COLUMN` is a metadata mutation, and a column with the dropped name can be added again
-- right away. A read then returns the new column's default, while the part still carries the statistics
-- of the dropped column under that name.

DROP TABLE IF EXISTS t_05200_drop;
CREATE TABLE t_05200_drop (a UInt64, b UInt64 STATISTICS(basic), s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_05200_drop;
INSERT INTO t_05200_drop SELECT number, number + 1000, 'x' FROM numbers(100);

ALTER TABLE t_05200_drop DROP COLUMN b SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_05200_drop ADD COLUMN b UInt64 DEFAULT 7 STATISTICS(basic);

SELECT 'the drop is pending', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_05200_drop' AND NOT is_done;

SELECT 'the values a read sees', groupUniqArray(b) FROM t_05200_drop;
SELECT 'the re-added column', count() FROM t_05200_drop WHERE b = 7;
SELECT 'the re-added column, without pruning', count() FROM t_05200_drop WHERE b = 7 SETTINGS use_statistics_for_part_pruning = 0;
SELECT 'the dropped values are gone', count() FROM t_05200_drop WHERE b = 1005;

SYSTEM START MERGES t_05200_drop;
ALTER TABLE t_05200_drop UPDATE s = s WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'after the mutation materialized', count() FROM t_05200_drop WHERE b = 7;
SELECT 'the statistics prune again', count() FROM t_05200_drop WHERE b = 1005;

DROP TABLE t_05200_drop;

-- The same for the top-k minmax index: the part still holds the index file built over the dropped
-- column's values, while the query orders by the re-added column. The part with the larger dropped
-- values holds the smaller live values, so a stale index picks the wrong part.

DROP TABLE IF EXISTS t_05200_topk;
CREATE TABLE t_05200_topk (a UInt64, b UInt64, INDEX idx b TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 10, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_05200_topk;
INSERT INTO t_05200_topk SELECT number, 5000 + number FROM numbers(100);
INSERT INTO t_05200_topk SELECT 100 + number, 1000 + number FROM numbers(100);

ALTER TABLE t_05200_topk DROP INDEX idx SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_05200_topk DROP COLUMN b SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_05200_topk ADD COLUMN b UInt64 DEFAULT a + 7;
ALTER TABLE t_05200_topk ADD INDEX idx b TYPE minmax GRANULARITY 1;

SELECT 'the drop is pending', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_05200_topk' AND NOT is_done;

SELECT 'the top row', a, b FROM t_05200_topk ORDER BY b DESC LIMIT 1;
SELECT 'the top row, without the index', a, b FROM t_05200_topk ORDER BY b DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 0;

DROP TABLE t_05200_topk;

-- The regular skip-index path has the same blind spot: a `WHERE` on the re-added column must not be
-- pruned by the index file built over the dropped column.

DROP TABLE IF EXISTS t_05200_skip;
CREATE TABLE t_05200_skip (a UInt64, b UInt64, INDEX idx b TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 10, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_05200_skip;
INSERT INTO t_05200_skip SELECT number, 5000 + number FROM numbers(100);
INSERT INTO t_05200_skip SELECT 100 + number, 1000 + number FROM numbers(100);

ALTER TABLE t_05200_skip DROP INDEX idx SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_05200_skip DROP COLUMN b SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_05200_skip ADD COLUMN b UInt64 DEFAULT a + 7;
ALTER TABLE t_05200_skip ADD INDEX idx b TYPE minmax GRANULARITY 1;

SELECT 'a filter on the re-added column', count() FROM t_05200_skip WHERE b BETWEEN 100 AND 110;
SELECT 'a filter on the re-added column, without the index', count() FROM t_05200_skip WHERE b BETWEEN 100 AND 110 SETTINGS use_skip_indexes = 0;

DROP TABLE t_05200_skip;

-- The query condition cache is keyed by the part name, which a pending metadata mutation does not
-- change: an entry recorded for the dropped column must not prune marks of the re-added one.

DROP TABLE IF EXISTS t_05200_qcc;
CREATE TABLE t_05200_qcc (a UInt64, b UInt64, s String) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 10, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_05200_qcc;
INSERT INTO t_05200_qcc SELECT number, number + 1000, 'x' FROM numbers(100);

SELECT 'before the drop', count() FROM t_05200_qcc WHERE b = 7 SETTINGS use_query_condition_cache = 1;

ALTER TABLE t_05200_qcc DROP COLUMN b SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_05200_qcc ADD COLUMN b UInt64 DEFAULT 7;

SELECT 'the re-added column', count() FROM t_05200_qcc WHERE b = 7 SETTINGS use_query_condition_cache = 1;
SELECT 'the re-added column, without the cache', count() FROM t_05200_qcc WHERE b = 7 SETTINGS use_query_condition_cache = 0;

DROP TABLE t_05200_qcc;
