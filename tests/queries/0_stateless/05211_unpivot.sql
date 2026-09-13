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
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM t UNPIVOT (v FOR k IN (a, b))'), '"unpivot_columns"', '"unpivot_columns_typo"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM t UNPIVOT (v FOR k IN (a, b))'), '"unpivot_name_name"', '"unpivot_name_name_typo"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM t UNPIVOT (v FOR k IN (a, b)) AS u'), '"unpivot_alias":"u"', '"unpivot_alias":1')); -- { serverError BAD_ARGUMENTS }

SELECT '-- errors';
SELECT * FROM monthly_sales UNPIVOT (sales FOR month IN (nope)); -- { serverError UNKNOWN_IDENTIFIER }
SELECT * FROM monthly_sales UNPIVOT (sales FOR month IN ()); -- { clientError SYNTAX_ERROR }
SELECT * FROM monthly_sales UNPIVOT (sales FOR month IN (jan + 1)); -- { clientError SYNTAX_ERROR }
SELECT * FROM monthly_sales UNPIVOT (sales FOR month); -- { clientError SYNTAX_ERROR }
SELECT * FROM monthly_sales UNPIVOT (sales FOR month IN (jan, feb)) SETTINGS enable_analyzer = 0; -- { serverError UNSUPPORTED_METHOD }

-- `UNPIVOT` is still usable as an identifier.
SELECT 1 AS unpivot, unpivot;
SELECT number FROM numbers(1) AS unpivot ORDER BY unpivot.number;
