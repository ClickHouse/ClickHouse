-- Regression test: a backquoted lambda argument whose name contains a dot
-- (e.g. `__table1.`) used to abort the server with Logical error '!part.empty()'
-- when the query tree was converted back to AST for an error message.
-- The lambda argument name is atomic, so it must not be split on '.'.

-- A type mismatch forces the analyzer to format the offending expression as AST,
-- which is where the abort happened. Now it must be a clean exception, server stays up.
SELECT arrayMap(`__table1.` -> (`__table1.`, `__table1.`), [1, 2]) :: Map(Point, DateTime); -- { serverError TYPE_MISMATCH }
SELECT arrayMap(`x.` -> (`x.`, `x.`), [1, 2]) :: Map(Point, DateTime); -- { serverError TYPE_MISMATCH }
SELECT arrayMap(`.x` -> (`.x`, `.x`), [1, 2]) :: Map(Point, DateTime); -- { serverError TYPE_MISMATCH }
SELECT arrayMap(`a..b` -> (`a..b`, `a..b`), [1, 2]) :: Map(Point, DateTime); -- { serverError TYPE_MISMATCH }
SELECT arrayMap(`a.b` -> (`a.b`, `a.b`), [1, 2]) :: Map(Point, DateTime); -- { serverError TYPE_MISMATCH }

-- A dotted lambda argument name binds correctly and is usable in the body.
SELECT arrayMap(`__table1.` -> `__table1.` + 100, [1, 2, 3]);
SELECT arrayMap(`a.b` -> `a.b` * 2, [10, 20]);

-- The original report: a column ALIAS whose expression has a dotted lambda argument
-- and a type that mismatches the declared column type. Used to abort during CREATE.
CREATE TABLE t_04367 (x UInt8, c Map(Point, DateTime) ALIAS arrayMap(`__table1.` -> toString(x), [0])) ENGINE = MergeTree ORDER BY x; -- { serverError TYPE_MISMATCH }

SELECT 1;
