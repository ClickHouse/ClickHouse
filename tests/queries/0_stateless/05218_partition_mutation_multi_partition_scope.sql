-- Regression test for the multi-partition form of a partition-scoped mutation,
-- `ALTER TABLE ... UPDATE/DELETE ... IN PARTITION p1, p2 WHERE ...`: it must be handled exactly
-- like the single-partition form.
--
-- 1. The mutation is local to the listed partitions: a part of an unlisted partition keeps its
--    name (and thus its data version) instead of being cloned to a new mutation version.
-- 2. The scope is pinned at creation by rewriting every listed `IN PARTITION <value>` into
--    `IN PARTITION ID '<id>'`, so a pending mutation survives a key-safe partition key type
--    change (e.g. `Enum8 -> Int8`) both on reload and on execution.

DROP TABLE IF EXISTS t_05218;

CREATE TABLE t_05218 (p Enum8('a' = 1, 'b' = 2, 'c' = 3), n Int64)
ENGINE = MergeTree PARTITION BY p ORDER BY tuple();

INSERT INTO t_05218 VALUES ('a', 1);
INSERT INTO t_05218 VALUES ('b', 2);
INSERT INTO t_05218 VALUES ('c', 3);

SELECT 'parts before the mutation';
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_05218' AND active ORDER BY name;

ALTER TABLE t_05218 UPDATE n = n + 100 IN PARTITION 'a', 'b' WHERE 1 SETTINGS mutations_sync = 1;

-- The persisted command is scoped to the resolved partition ids, not to the original literals.
SELECT command FROM system.mutations WHERE database = currentDatabase() AND table = 't_05218' ORDER BY mutation_id;

-- Only the parts of partitions 1 and 2 got a new mutation version; the part of partition 3 is intact.
SELECT 'parts after the mutation';
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_05218' AND active ORDER BY name;
SELECT p, n FROM t_05218 ORDER BY p, n;

-- Now keep a multi-partition mutation pending across a partition key type change and a restart.
SYSTEM STOP MERGES t_05218;

ALTER TABLE t_05218 DELETE IN PARTITION 'b', 'c' WHERE n = 3;

-- A key-safe metadata change of the partition key column: `Enum8 -> Int8` keeps the numeric
-- on-disk partition id, but re-parsing the literals 'b' and 'c' as `Int8` would throw.
ALTER TABLE t_05218 MODIFY COLUMN p Int8 SETTINGS alter_sync = 2;

-- Simulate a restart so that the mutation file is read back by `loadMutations`.
DETACH TABLE t_05218;
ATTACH TABLE t_05218;

-- `SYSTEM STOP MERGES` does not survive the restart, so the mutation may run right away; select it
-- by its command rather than by `NOT is_done`.
SELECT command FROM system.mutations WHERE database = currentDatabase() AND table = 't_05218' AND command LIKE '%DELETE%' ORDER BY mutation_id;

SYSTEM START MERGES t_05218;

-- The pending mutation is executed after the restart and affects only the partitions it was scoped to.
ALTER TABLE t_05218 UPDATE n = n IN PARTITION 2, 3 WHERE 1 SETTINGS mutations_sync = 1;

SELECT p, n FROM t_05218 ORDER BY p, n;

SELECT 'parts after the pending mutation';
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_05218' AND active ORDER BY name;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_05218' AND NOT is_done;

DROP TABLE t_05218;
