-- With the old analyzer the WHERE -> PREWHERE move is done by `InterpreterSelectQuery`, which
-- also reads the `MergeTree` parts for the condition selectivity estimator out of the storage
-- snapshot. `Memory` has its own type of snapshot data, and reading it as the `MergeTree` one
-- used to dereference the row count as a pointer.

SET enable_analyzer = 0;
SET optimize_move_to_prewhere = 1;

DROP TABLE IF EXISTS t_memory_old_analyzer;

CREATE TABLE t_memory_old_analyzer (k UInt64, v String) ENGINE = Memory;

-- One row, so the row count is read as the pointer 0x1, which passes a null check.
INSERT INTO t_memory_old_analyzer VALUES (1, 'a');

SELECT count() FROM t_memory_old_analyzer WHERE k = 1;
SELECT v FROM t_memory_old_analyzer WHERE k = 1;
SELECT v FROM t_memory_old_analyzer WHERE k = 2;

INSERT INTO t_memory_old_analyzer SELECT number, toString(number) FROM numbers(2, 100);

SELECT count() FROM t_memory_old_analyzer WHERE k % 10 = 0 AND v != '';
SELECT v FROM t_memory_old_analyzer WHERE k = 50;

DROP TABLE t_memory_old_analyzer;
