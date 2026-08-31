-- https://github.com/ClickHouse/ClickHouse/issues/117022
-- `arrayJoin` changes the number of rows, while `CheckConstraintsTransform` indexes the result column
-- positionally against the rows of the block being inserted: a longer result read past the end of the
-- block's columns (a nonsensical `TOO_LARGE_ARRAY_SIZE`, or an abort in a debug build) and a shorter
-- one blamed the violation on the wrong row. `arrayJoin` is rejected for skip indexes, keys,
-- mutations, `PREWHERE` and row policies for the same reason.

DROP TABLE IF EXISTS t_constraint_array_join;
CREATE TABLE t_constraint_array_join (id Int32, arr Array(Int32), CONSTRAINT c CHECK arrayJoin(arr) > 0) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE t_constraint_array_join (id Int32, arr Array(Int32), CONSTRAINT c CHECK id > 0 AND arrayJoin(arr) > 0) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

-- An ordinary constraint still works.
SELECT 'ordinary constraint';
CREATE TABLE t_constraint_array_join (id Int32, arr Array(Int32), CONSTRAINT c CHECK arr[1] > 0) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_constraint_array_join VALUES (1, [1, 1]);
SELECT count() FROM t_constraint_array_join;
INSERT INTO t_constraint_array_join VALUES (2, [-5]); -- { serverError VIOLATED_CONSTRAINT }
SELECT count() FROM t_constraint_array_join;

DROP TABLE t_constraint_array_join;
