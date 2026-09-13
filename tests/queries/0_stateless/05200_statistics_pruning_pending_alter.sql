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
