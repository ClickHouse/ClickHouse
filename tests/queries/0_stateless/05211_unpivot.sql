SET enable_analyzer = 1;

CREATE TABLE monthly_sales (empid UInt32, dept String, jan Int32, feb Nullable(Int32), mar Int32) ENGINE = Memory;
INSERT INTO monthly_sales VALUES (1, 'electronics', 100, 200, 300), (2, 'clothes', 10, NULL, 30);

-- UNPIVOT is experimental and rejected until it is enabled.
SELECT * FROM monthly_sales UNPIVOT (sales FOR month IN (jan, feb, mar)); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_unpivot = 1;

SELECT '-- the listed columns become rows, the rest are kept';
SELECT * FROM monthly_sales UNPIVOT (sales FOR month IN (jan, feb, mar)) ORDER BY empid, month;

SELECT '-- a column can be renamed, and only the listed ones are turned into rows';
SELECT * FROM monthly_sales UNPIVOT (sales FOR month IN (jan AS january, feb AS february)) ORDER BY empid, month;

SELECT '-- NULL values are dropped by default and kept with INCLUDE NULLS';
SELECT empid, month, sales FROM monthly_sales UNPIVOT (sales FOR month IN (jan, feb)) ORDER BY empid, month;
SELECT empid, month, sales FROM monthly_sales UNPIVOT EXCLUDE NULLS (sales FOR month IN (jan, feb)) ORDER BY empid, month;
SELECT empid, month, sales FROM monthly_sales UNPIVOT INCLUDE NULLS (sales FOR month IN (jan, feb)) ORDER BY empid, month;

SELECT '-- the name column is a String, the value column takes the common type of the listed columns';
SELECT toTypeName(month), toTypeName(sales) FROM monthly_sales UNPIVOT (sales FOR month IN (jan, feb)) LIMIT 1;

SELECT '-- the result can be aliased and referred to by that alias';
SELECT u.month, sum(u.sales) FROM monthly_sales UNPIVOT (sales FOR month IN (jan, mar)) AS u GROUP BY u.month ORDER BY u.month;

SELECT '-- a subquery, a table function and a single column all work as the source';
SELECT * FROM (SELECT 1 AS a, 2 AS b) UNPIVOT (value FOR name IN (a, b)) ORDER BY name;
SELECT * FROM numbers(2) UNPIVOT (value FOR name IN (number)) ORDER BY value;
SELECT * FROM monthly_sales UNPIVOT (sales FOR month IN (jan)) ORDER BY empid;

SELECT '-- the clause composes with joins on either side, and with ARRAY JOIN';
SELECT u.empid, u.month, t.w FROM monthly_sales UNPIVOT (sales FOR month IN (jan, mar)) AS u JOIN (SELECT 'jan' AS k, 1 AS w) AS t ON u.month = t.k ORDER BY ALL;
SELECT t.k, u.empid, u.month FROM (SELECT 'jan' AS k) AS t JOIN monthly_sales UNPIVOT (sales FOR month IN (jan, mar)) AS u ON u.month = t.k ORDER BY ALL;
SELECT count() FROM (SELECT 1 AS x) AS t, monthly_sales UNPIVOT (sales FOR month IN (jan, mar)) AS u;
SELECT month, sales, a FROM monthly_sales UNPIVOT (sales FOR month IN (jan)) ARRAY JOIN [1, 2] AS a ORDER BY ALL;

SELECT '-- the name of an unaliased source still qualifies the result';
SELECT monthly_sales.month, monthly_sales.sales FROM monthly_sales UNPIVOT (sales FOR month IN (jan, mar)) ORDER BY ALL;
SELECT monthly_sales.* FROM monthly_sales UNPIVOT (sales FOR month IN (jan)) ORDER BY ALL;

SELECT '-- the name of a CTE qualifies the result too';
WITH cte_05210 AS (SELECT empid, jan, mar FROM monthly_sales)
SELECT cte_05210.month, cte_05210.sales FROM cte_05210 UNPIVOT (sales FOR month IN (jan, mar)) ORDER BY ALL;
WITH cte_05210 AS (SELECT empid, jan FROM monthly_sales)
SELECT cte_05210.* FROM cte_05210 UNPIVOT (sales FOR month IN (jan)) ORDER BY ALL;
WITH cte_05210 AS (SELECT jan FROM monthly_sales)
SELECT u.month FROM cte_05210 UNPIVOT (sales FOR month IN (jan)) AS u ORDER BY ALL;

SELECT '-- an alias on the source names the result, and an alias on the clause wins over it';
SELECT s.month, s.sales FROM monthly_sales AS s UNPIVOT (sales FOR month IN (jan, mar)) ORDER BY ALL;
SELECT s.empid, s.month FROM monthly_sales AS s UNPIVOT (sales FOR month IN (jan)) ORDER BY ALL;
SELECT u.month FROM monthly_sales AS s UNPIVOT (sales FOR month IN (jan)) AS u ORDER BY ALL;

SELECT '-- an enclosing WITH does not reach into the rewrite';
-- The rewrite names its arrays after the columns the clause asks for, so there is no helper name of
-- its own for an alias to bind to, under any value of the setting that scopes a WITH.
WITH 'x' AS __unpivot_name, 1 AS __unpivot_value
SELECT month, sales FROM monthly_sales UNPIVOT (sales FOR month IN (jan, mar)) ORDER BY ALL;
WITH 'x' AS __unpivot_name, 1 AS __unpivot_value
SELECT month, sales FROM monthly_sales UNPIVOT (sales FOR month IN (jan, mar)) ORDER BY ALL
SETTINGS enable_scopes_for_with_statement = 0;

SELECT '-- the clause survives formatting';
SELECT formatQuery('SELECT * FROM monthly_sales UNPIVOT (sales FOR month IN (jan, feb))');
SELECT formatQuery('SELECT * FROM monthly_sales UNPIVOT INCLUDE NULLS (sales FOR month IN (jan AS x, feb)) AS u');
SELECT formatQuery('SELECT * FROM monthly_sales FINAL SAMPLE 1 / 2 UNPIVOT (sales FOR month IN (jan, feb)) AS u');
SELECT formatQuery('SELECT * FROM monthly_sales STREAM UNPIVOT (sales FOR month IN (jan, feb))');

SELECT '-- the clause survives an AST JSON round trip';
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM monthly_sales UNPIVOT (sales FOR month IN (jan, feb))'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM monthly_sales UNPIVOT INCLUDE NULLS (sales FOR month IN (jan AS x, feb)) AS u'));

-- A partial or misshapen clause in hand-written `clickhouse_json` is rejected instead of being
-- silently dropped on formatting or dereferenced as null by the analyzer.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM t UNPIVOT (v FOR k IN (a, b))'), '"columns"', '"columns_typo"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM t UNPIVOT (v FOR k IN (a, b))'), '"name_name"', '"name_name_typo"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM t UNPIVOT (v FOR k IN (a, b)) AS u'), '"result_alias":"u"', '"result_alias":1')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM t UNPIVOT (v FOR k IN (a, b))'), '"name":"a"', '"number":1')); -- { serverError BAD_ARGUMENTS }

SELECT '-- errors';
SELECT * FROM monthly_sales UNPIVOT (sales FOR month IN (nope)); -- { serverError UNKNOWN_IDENTIFIER }
-- A listed column has to be a column of the source, not something an enclosing scope happens to bind.
WITH 42 AS jan SELECT * FROM (SELECT 1 AS x) UNPIVOT (sales FOR month IN (jan)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM monthly_sales UNPIVOT (sales FOR month IN ()); -- { clientError SYNTAX_ERROR }
SELECT * FROM monthly_sales UNPIVOT (sales FOR month IN (jan + 1)); -- { clientError SYNTAX_ERROR }
SELECT * FROM monthly_sales UNPIVOT (sales FOR month); -- { clientError SYNTAX_ERROR }
-- The old analysis path refuses the clause rather than ignoring it. It cannot be reached from here:
-- since 26.9 a query cannot turn the analyzer off, it is only a query another server sent that keeps
-- the old analysis alive.

-- `UNPIVOT` is not a reserved word: it is only the clause where the clause can start, so an alias
-- named after it keeps working, with or without `AS`.
SELECT 1 AS unpivot, unpivot;
SELECT 1 unpivot;
SELECT number FROM numbers(1) AS unpivot ORDER BY unpivot.number;
SELECT number FROM numbers(1) unpivot ORDER BY unpivot.number;
-- An alias named after the clause, followed by a column alias list, is not the clause: the clause is
-- only recognised by its whole head, `UNPIVOT [INCLUDE|EXCLUDE NULLS] (<name> FOR <name> IN (`.
SELECT a, b FROM (SELECT 1 AS x, 2 AS y) unpivot (a, b);
SELECT a FROM (SELECT 1 AS x) AS unpivot (a);
