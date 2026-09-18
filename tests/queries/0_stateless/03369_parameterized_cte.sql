-- Basic parameterized CTE
WITH t AS (SELECT {v:String} AS value) SELECT * FROM t(v = 'Hello, world!');

-- Multiple parameters
WITH t AS (SELECT {a:UInt32} + {b:UInt32} AS result) SELECT * FROM t(a = 1, b = 2);

-- The CTE name qualifies the columns when the invocation carries no alias
WITH t AS (SELECT {v:String} AS value) SELECT t.value FROM t(v = 'qualified');

-- An explicit alias is kept
WITH t AS (SELECT {v:String} AS value) SELECT x.value FROM t(v = 'aliased') AS x;

-- A CTE declared by an enclosing query is visible in a child subquery
WITH t AS (SELECT {v:String} AS value) SELECT * FROM (SELECT * FROM t(v = 'from outer scope'));

-- Two levels of subquery
WITH t AS (SELECT {v:String} AS value) SELECT * FROM (SELECT * FROM (SELECT * FROM t(v = 'from two levels up')));

-- The body is analyzed: nested table expressions and aliases inside it resolve
WITH t AS (SELECT n * 2 AS doubled FROM (SELECT {n:UInt32} AS n)) SELECT doubled FROM t(n = 21);

-- The body may be a union
WITH t AS (SELECT {a:UInt32} AS x UNION ALL SELECT {b:UInt32}) SELECT sum(x) FROM t(a = 1, b = 2);

-- The same CTE can be invoked twice with different arguments
WITH t AS (SELECT {a:UInt32} + {b:UInt32} AS result) SELECT x.result + y.result AS total FROM t(a = 1, b = 2) AS x, t(a = 10, b = 20) AS y;

-- An inner declaration shadows an outer one of the same name
WITH t AS (SELECT {v:String} AS value) SELECT * FROM (WITH t AS (SELECT concat('inner ', {v:String}) AS value) SELECT * FROM t(v = 'wins'));

SET param_greeting = 'Hello';
SET param_subject = 'world';

-- A value supplied by the invocation wins over a query parameter of the same name
WITH t AS (SELECT {subject:String} AS value) SELECT * FROM t(subject = 'ClickHouse');

-- A query parameter supplies only what the invocation left out
WITH t AS (SELECT concat({greeting:String}, ', ', {subject:String}) AS value) SELECT * FROM t(subject = 'ClickHouse');

-- A parameter that neither the invocation nor a query parameter supplies is an error
WITH t AS (SELECT {missing:String} AS value) SELECT * FROM t(subject = 'ClickHouse'); -- { serverError UNKNOWN_QUERY_PARAMETER }

-- A reference to a parameterized CTE from inside its own body does not expand it
WITH t AS (SELECT {v:String} AS value FROM t(v = 'recursive')) SELECT * FROM t(v = 'x'); -- { serverError UNKNOWN_FUNCTION }

-- A parameterized CTE that is never invoked keeps failing on the unset parameter
WITH t AS (SELECT {unset:String} AS value) SELECT 1; -- { serverError UNKNOWN_QUERY_PARAMETER }
