-- A query parameter alias `AS {name:Identifier}` works inside parentheses, function arguments, CAST and a
-- parenthesized JOIN like a plain alias, and is rejected where a plain alias is.

SET param_p = 'y';
SET param_q = 'j';
SET param_x = '5';

SELECT (1 AS {p:Identifier}), y + 1 FORMAT TSVWithNames;
SELECT plus(1 AS {p:Identifier}, 1), y FORMAT TSVWithNames;
SELECT y FROM (SELECT (1 AS {p:Identifier}));
SELECT number FROM numbers(3) WHERE (number AS {p:Identifier}) > 0 AND y < 2;
SELECT ({x:UInt8} AS {p:Identifier}), y;
SELECT ((1, 2) AS {p:Identifier}), y;
SELECT (0.1 AS {p:Identifier})::Decimal(10, 2), y;
SELECT CAST(0.1 AS {p:Identifier}, 'Decimal(10, 2)'), y;
SELECT CAST(1 AS {p:Identifier} AS String), y;
SELECT j.x FROM ((SELECT 1 AS x) AS l CROSS JOIN (SELECT 2 AS z) AS r) AS {q:Identifier};
SELECT (1 AS {p:Identifier}) AS b, b;
SELECT (1 AS {unset:Identifier}); -- { serverError UNKNOWN_QUERY_PARAMETER }

CREATE TABLE t (a Int32, b Int32) ENGINE = MergeTree ORDER BY (a AS {p:Identifier}); -- { clientError SYNTAX_ERROR }
CREATE TABLE t (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
ALTER TABLE t DELETE WHERE (a = 1 AS {p:Identifier}); -- { clientError SYNTAX_ERROR }
ALTER TABLE t UPDATE b = 2 WHERE (a = 1 AS {p:Identifier}); -- { clientError SYNTAX_ERROR }
DELETE FROM t WHERE (a = 1 AS {p:Identifier}); -- { clientError SYNTAX_ERROR }
UPDATE t SET b = 2 WHERE (a = 1 AS {p:Identifier}); -- { clientError SYNTAX_ERROR }
CREATE ROW POLICY p ON t USING (a = 1 AS {p:Identifier}); -- { clientError SYNTAX_ERROR }
DROP ROW POLICY IF EXISTS p ON t;
DROP TABLE t;

-- `TABLE OVERRIDE` keys reject a top-level alias of either kind and keep a nested one.
SELECT formatQuery('CREATE DATABASE d ENGINE = MaterializedPostgreSQL(''h:5432'', ''db'', ''u'', ''p'') TABLE OVERRIDE t (PARTITION BY (a AS {p:Identifier}))'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE DATABASE d ENGINE = MaterializedPostgreSQL(''h:5432'', ''db'', ''u'', ''p'') TABLE OVERRIDE t (ORDER BY (a AS y))'); -- { serverError SYNTAX_ERROR }
SELECT formatQuerySingleLine('CREATE DATABASE d ENGINE = MaterializedPostgreSQL(''h:5432'', ''db'', ''u'', ''p'') TABLE OVERRIDE t (PARTITION BY (a) ORDER BY ((a AS y) + 1))');
