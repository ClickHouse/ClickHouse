DROP TABLE IF EXISTS t_lightweight_mut_2;

SET apply_mutations_on_fly = 1;

CREATE TABLE t_lightweight_mut_2 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES t_lightweight_mut_2;
INSERT INTO t_lightweight_mut_2 VALUES (10, 20);

ALTER TABLE t_lightweight_mut_2 UPDATE v = rand() WHERE 1;

SELECT * FROM t_lightweight_mut_2; -- { serverError BAD_ARGUMENTS }
SELECT * FROM t_lightweight_mut_2 SETTINGS apply_mutations_on_fly = 0;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_lightweight_mut_2' AND NOT is_done AND NOT is_killed;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_lightweight_mut_2' FORMAT Null;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_lightweight_mut_2' AND NOT is_done AND NOT is_killed;

ALTER TABLE t_lightweight_mut_2 UPDATE v = (SELECT sum(number) FROM numbers(100)) WHERE 1;

SELECT * FROM t_lightweight_mut_2; -- { serverError BAD_ARGUMENTS }
SELECT * FROM t_lightweight_mut_2 SETTINGS apply_mutations_on_fly = 0;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_lightweight_mut_2' AND NOT is_done AND NOT is_killed;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_lightweight_mut_2' FORMAT Null;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_lightweight_mut_2' AND NOT is_done AND NOT is_killed;

DROP TABLE t_lightweight_mut_2;
