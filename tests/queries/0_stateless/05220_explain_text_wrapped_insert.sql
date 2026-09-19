-- Inline data is rejected through each wrapper.
SELECT formatQuery('EXPLAIN TEXT EXECUTE AS default INSERT INTO t VALUES (1)'); -- { serverError BAD_ARGUMENTS }
SELECT formatQuery('EXPLAIN TEXT SELECT 1 PARALLEL WITH INSERT INTO t VALUES (1)'); -- { serverError BAD_ARGUMENTS }
SELECT formatQuery('EXPLAIN TEXT EXPLAIN AST INSERT INTO t VALUES (1)'); -- { serverError BAD_ARGUMENTS }
SELECT formatQuery('EXPLAIN TEXT (EXECUTE AS default INSERT INTO t VALUES (1)) ONELINE'); -- { serverError BAD_ARGUMENTS }

-- A bare trailing `FORMAT` belongs to `EXPLAIN TEXT`.
EXPLAIN TEXT EXECUTE AS default INSERT INTO t SELECT 1 FORMAT JSONEachRow;
EXPLAIN TEXT SELECT 2 PARALLEL WITH INSERT INTO t SELECT 1 FORMAT JSONEachRow;

-- Parentheses preserve the source format.
EXPLAIN TEXT (EXECUTE AS default INSERT INTO t SELECT 1 FORMAT CSV) ONELINE;

-- The input format remains attached to the source.
EXPLAIN TEXT EXECUTE AS default INSERT INTO t SELECT * FROM input('x UInt8') FORMAT CSV ONELINE;

-- Consecutive formats retain their separate owners.
EXPLAIN TEXT EXECUTE AS default INSERT INTO t SELECT 1 FORMAT CSV FORMAT JSONEachRow;
