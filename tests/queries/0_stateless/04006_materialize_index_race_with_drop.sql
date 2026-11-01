-- Test that MATERIALIZE INDEX mutation does not get stuck
-- when the index is dropped before the mutation executes.
-- https://github.com/ClickHouse/ClickHouse/issues/38643

SET mutations_sync = 0;

DROP TABLE IF EXISTS t_materialize_index_race;

CREATE TABLE t_materialize_index_race (x UInt64)
ENGINE = MergeTree
ORDER BY x;

INSERT INTO t_materialize_index_race SELECT number FROM numbers(100);

-- Create an index, start materializing it, then kill the mutation and drop the index.
-- After this, the killed mutation should not get stuck retrying with "Unknown index" error.
ALTER TABLE t_materialize_index_race ADD INDEX idx x TYPE minmax GRANULARITY 1;
ALTER TABLE t_materialize_index_race MATERIALIZE INDEX idx;

-- Kill the MATERIALIZE INDEX mutation so DROP INDEX is allowed.
KILL MUTATION WHERE database = currentDatabase() AND table = 't_materialize_index_race' AND command LIKE '%MATERIALIZE INDEX%' FORMAT Null;

ALTER TABLE t_materialize_index_race DROP INDEX idx;

SELECT count() FROM t_materialize_index_race;

-- Also test the same scenario for projections.
ALTER TABLE t_materialize_index_race ADD PROJECTION proj (SELECT x ORDER BY x);
ALTER TABLE t_materialize_index_race MATERIALIZE PROJECTION proj;

KILL MUTATION WHERE database = currentDatabase() AND table = 't_materialize_index_race' AND command LIKE '%MATERIALIZE PROJECTION%' FORMAT Null;

ALTER TABLE t_materialize_index_race DROP PROJECTION proj;

SELECT count() FROM t_materialize_index_race;

-- Now test the more direct scenario: add index, materialize, drop index, and wait
-- for mutations to complete. The MATERIALIZE mutation should be silently skipped.
ALTER TABLE t_materialize_index_race ADD INDEX idx2 x TYPE minmax GRANULARITY 1;
ALTER TABLE t_materialize_index_race MATERIALIZE INDEX idx2;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_materialize_index_race' AND command LIKE '%MATERIALIZE INDEX%' FORMAT Null;
ALTER TABLE t_materialize_index_race DROP INDEX idx2 SETTINGS mutations_sync = 2;

-- Verify no stuck mutations remain.
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_materialize_index_race' AND NOT is_done;

DROP TABLE t_materialize_index_race;
