-- Tags: no-ordinary-database
-- Tag no-ordinary-database: the ATTACH spelling below needs a table UUID

-- A declared column type may not contain the state of a function that only works as a window
-- function: such a state has no serialization, so the column could never hold a value.

DROP TABLE IF EXISTS t_window_state;
DROP TABLE IF EXISTS t_window_state_ok;

CREATE TABLE t_window_state (c AggregateFunction(rank)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
-- A type wrapper does not hide it, and no setting opens the walk.
CREATE TABLE t_window_state (c Array(AggregateFunction(rank))) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
-- Nor does the argument list of another state.
CREATE TABLE t_window_state (c AggregateFunction(any, AggregateFunction(rank))) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
-- A combinator answers for the function it wraps, including the one that wraps several.
CREATE TABLE t_window_state (c AggregateFunction(rankState)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_window_state (c AggregateFunction(rankTuple, Tuple(UInt64))) ENGINE = Memory SETTINGS allow_rank_dense_rank_arguments = 1; -- { serverError BAD_ARGUMENTS }
-- A full-definition ATTACH declares a type rather than replaying a stored one.
ATTACH TABLE t_window_state UUID '00000000-1234-0000-0000-000000005257' (c AggregateFunction(rank)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
-- A table function's structure argument.
SELECT * FROM format('CSV', 'c AggregateFunction(rank)', ''); -- { serverError BAD_ARGUMENTS }
-- A type named in an expression.
SELECT CAST(NULL, 'AggregateFunction(rank)'); -- { serverError BAD_ARGUMENTS }

SELECT 'alter';
CREATE TABLE t_window_state_ok (x UInt64, c AggregateFunction(sum, UInt64)) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_window_state_ok ADD COLUMN w AggregateFunction(rank); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_window_state_ok MODIFY COLUMN c AggregateFunction(rank); -- { serverError BAD_ARGUMENTS }

SELECT 'ordinary aggregate state';
INSERT INTO t_window_state_ok SELECT 1, sumState(number) FROM numbers(4);
SELECT x, sumMerge(c) FROM t_window_state_ok GROUP BY x;
DETACH TABLE t_window_state_ok;
ATTACH TABLE t_window_state_ok;
SELECT count() FROM t_window_state_ok;

SELECT 'window functions';
SELECT rank() OVER (ORDER BY number) FROM numbers(3);

DROP TABLE t_window_state_ok;
