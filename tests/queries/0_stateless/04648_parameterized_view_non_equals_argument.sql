-- A parameterized-view parameter must be bound only by a top-level
-- `identifier = <constant expression>` argument of the view call, identically on the
-- legacy-AST path (`enable_analyzer = 0`, `EXPLAIN SYNTAX`) and on the query-tree path.
-- Every statement pins `enable_analyzer` explicitly, because the `old analyzer` CI jobs
-- link `users.d/analyzer.xml` and would otherwise make the convergence rows vacuous.

DROP TABLE IF EXISTS 04648_t;
DROP VIEW IF EXISTS 04648_pv;
DROP VIEW IF EXISTS 04648_pv2;

CREATE TABLE 04648_t (name String, x UInt32) ENGINE = Memory;
INSERT INTO 04648_t VALUES ('a', 1), ('b', 2), ('!', 9);
CREATE VIEW 04648_pv AS SELECT * FROM 04648_t WHERE name = {name:String};
CREATE VIEW 04648_pv2 AS SELECT * FROM 04648_t WHERE name = {name:String} AND x = {xx:UInt32};

SELECT 'positive controls';

-- A genuine assignment binds the parameter, in every accepted spelling.
SELECT * FROM 04648_pv(name = 'a') SETTINGS enable_analyzer = 0;
SELECT * FROM 04648_pv(name = 'a') SETTINGS enable_analyzer = 1;
SELECT * FROM 04648_pv(equals(name, 'a')) SETTINGS enable_analyzer = 0;
SELECT * FROM 04648_pv(equals(name, 'a')) SETTINGS enable_analyzer = 1;

-- A parametric spelling keeps its parameters in a separate child of the `ASTFunction`, so
-- the shape check must key on `arguments`, not on the child count. Both paths accept it.
SELECT * FROM 04648_pv(equals(7)(name, 'a')) SETTINGS enable_analyzer = 0;
SELECT * FROM 04648_pv(equals(7)(name, 'a')) SETTINGS enable_analyzer = 1;

-- Several assignments, and non-literal constant right-hand sides.
SELECT * FROM 04648_pv2(name = 'a', xx = 1) SETTINGS enable_analyzer = 0;
SELECT * FROM 04648_pv2(name = 'a', xx = 1) SETTINGS enable_analyzer = 1;
SELECT * FROM 04648_pv(name = (SELECT 'a')) SETTINGS enable_analyzer = 0;
SELECT * FROM 04648_pv(name = (SELECT 'a')) SETTINGS enable_analyzer = 1;
SELECT * FROM 04648_pv(name = CAST('a', 'String')) SETTINGS enable_analyzer = 0;
SELECT * FROM 04648_pv(name = CAST('a', 'String')) SETTINGS enable_analyzer = 1;

-- The last assignment to the same parameter wins, on both paths.
SELECT * FROM 04648_pv(name = 'a', name = 'b') SETTINGS enable_analyzer = 0;
SELECT * FROM 04648_pv(name = 'a', name = 'b') SETTINGS enable_analyzer = 1;

SELECT 'non-equals arguments';

-- A non-`equals` function argument is not an assignment: it must not bind the parameter
-- from its own second argument. `concat` used to bind `name = '!'` on the legacy path.
SELECT * FROM 04648_pv(concat(name, '!')) SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(concat(name, '!')) SETTINGS enable_analyzer = 1; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(name != 'a') SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(name != 'a') SETTINGS enable_analyzer = 1; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(name > 'a') SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(name > 'a') SETTINGS enable_analyzer = 1; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(name + 1) SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(name + 1) SETTINGS enable_analyzer = 1; -- { serverError UNKNOWN_QUERY_PARAMETER }

-- Function names are case sensitive here: only the canonical `equals` is an assignment.
SELECT * FROM 04648_pv(EQUALS(name, 'a')) SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(EQUALS(name, 'a')) SETTINGS enable_analyzer = 1; -- { serverError UNKNOWN_QUERY_PARAMETER }

SELECT 'positional arguments';

-- A positional call has no assignment at all: the view call's own argument list used to be
-- read as if it were an `equals` argument list, binding `name = 'a'` on the legacy path.
SELECT * FROM 04648_pv(name, 'a') SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(name, 'a') SETTINGS enable_analyzer = 1; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv2(name = 'a', concat(xx, 'q')) SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv2(name = 'a', concat(xx, 'q')) SETTINGS enable_analyzer = 1; -- { serverError UNKNOWN_QUERY_PARAMETER }

SELECT 'nested assignments';

-- An assignment nested inside a rejected argument must not be collected either: only the
-- top-level arguments of the view call are inspected.
SELECT * FROM 04648_pv((name = 'a') AND 1) SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv((name = 'a') AND 1) SETTINGS enable_analyzer = 1; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(tuple(name = 'a')) SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(tuple(name = 'a')) SETTINGS enable_analyzer = 1; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(if(1, name = 'a', name = 'b')) SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(if(1, name = 'a', name = 'b')) SETTINGS enable_analyzer = 1; -- { serverError UNKNOWN_QUERY_PARAMETER }

SELECT 'already rejected before';

-- Shapes that both paths already rejected, pinned as unchanged.
SELECT * FROM 04648_pv() SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv() SETTINGS enable_analyzer = 1; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(equals()) SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(equals()) SETTINGS enable_analyzer = 1; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(other = 1) SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(other = 1) SETTINGS enable_analyzer = 1; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv('a' = name) SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv('a' = name) SETTINGS enable_analyzer = 1; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(04648_t.name = 'a') SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT * FROM 04648_pv(04648_t.name = 'a') SETTINGS enable_analyzer = 1; -- { serverError UNKNOWN_QUERY_PARAMETER }

SELECT 'with alias right-hand side';

-- PRE-EXISTING and out of scope: the legacy path cannot evaluate a `WITH` alias to a
-- constant, so this diverges for a reason unrelated to argument-shape validation
-- (constant-expression evaluation). Pinned here as unchanged.
WITH 'a' AS s SELECT * FROM 04648_pv(name = s) SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_QUERY_PARAMETER }
WITH 'a' AS s SELECT * FROM 04648_pv(name = s) SETTINGS enable_analyzer = 1;

SELECT 'explain syntax';

-- `EXPLAIN SYNTAX` reaches the same collector, so it must stop rendering a plan that
-- execution refuses to run. These reject identically under both analyzers; the accepted
-- rows are pinned because the two analyzers render the projection list differently.
EXPLAIN SYNTAX SELECT * FROM 04648_pv(concat(name, '!')); -- { serverError UNKNOWN_QUERY_PARAMETER }
EXPLAIN SYNTAX SELECT * FROM 04648_pv(name, 'a'); -- { serverError UNKNOWN_QUERY_PARAMETER }
EXPLAIN SYNTAX SELECT * FROM 04648_pv(tuple(name = 'a')); -- { serverError UNKNOWN_QUERY_PARAMETER }
EXPLAIN SYNTAX SELECT * FROM 04648_pv(name = 'a') SETTINGS enable_analyzer = 1;
EXPLAIN SYNTAX SELECT * FROM 04648_pv(equals(7)(name, 'a')) SETTINGS enable_analyzer = 1;

DROP VIEW 04648_pv2;
DROP VIEW 04648_pv;
DROP TABLE 04648_t;
