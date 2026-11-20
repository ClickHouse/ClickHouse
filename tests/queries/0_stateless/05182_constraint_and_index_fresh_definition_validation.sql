-- The validations that reject a definition which could never be evaluated apply to fresh user input
-- only - `CREATE TABLE`, a full-definition `ATTACH TABLE`, `CREATE TABLE ... AS` and `ALTER`.
-- Metadata read back from disk is loaded without them, so a table whose definition was accepted by an
-- older server keeps loading and keeps surviving unrelated `ALTER`s.

DROP TABLE IF EXISTS t_row_multiplying_constraint;
DROP TABLE IF EXISTS t_index_over_alias;
DROP TABLE IF EXISTS t_set;

-- A `CHECK` constraint that multiplies rows is rejected on a plain `CREATE` ...
CREATE TABLE t_row_multiplying_constraint (x UInt64, arr Array(UInt64), CONSTRAINT c CHECK arrayJoin(arr) < 10)
ENGINE = MergeTree ORDER BY x; -- { serverError INCORRECT_QUERY }

-- ... and on `ALTER TABLE ... ADD CONSTRAINT`.
CREATE TABLE t_row_multiplying_constraint (x UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY x;
ALTER TABLE t_row_multiplying_constraint ADD CONSTRAINT c CHECK arrayJoin(arr) < 10; -- { serverError INCORRECT_QUERY }

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_row_multiplying_constraint';

-- An `IN` over a table cannot be evaluated outside of a `SELECT` pipeline, so a skip index over an
-- `ALIAS` column that hides one is rejected for a fresh definition - the check runs after alias
-- expansion, not only on the raw index expression.
CREATE TABLE t_set (id UInt64) ENGINE = Set;
CREATE TABLE t_index_over_alias (x UInt64, y UInt64, a ALIAS x IN t_set, INDEX idx a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY x; -- { serverError BAD_ARGUMENTS }

-- An ordinary skip index over an `ALIAS` column keeps working, including across an unrelated `ALTER`
-- that rebuilds every index from its stored definition.
CREATE TABLE t_index_over_alias (x UInt64, y UInt64, a ALIAS x + y, INDEX idx a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY x;
INSERT INTO t_index_over_alias (x, y) VALUES (1, 2), (3, 4);
ALTER TABLE t_index_over_alias ADD COLUMN z UInt64 DEFAULT 0;
SELECT x, y, a, z FROM t_index_over_alias ORDER BY x;

-- `ALTER TABLE ... ADD INDEX` is fresh input as well, so a matcher is rejected there.
ALTER TABLE t_index_over_alias ADD INDEX idx2 (COLUMNS('^x')) TYPE minmax GRANULARITY 1; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_row_multiplying_constraint;
DROP TABLE t_index_over_alias;
DROP TABLE t_set;
