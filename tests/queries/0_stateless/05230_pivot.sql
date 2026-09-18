DROP TABLE IF EXISTS pivot_static;
CREATE TABLE pivot_static
(
    region String,
    quarter Nullable(String),
    amount UInt64
)
ENGINE = Memory;

INSERT INTO pivot_static VALUES
    ('east', 'Q1', 10),
    ('east', 'Q2', 20),
    ('west', 'Q1', 5),
    ('west', NULL, 7);

-- Basic static pivot. Only region remains as an implicit grouping key.
SELECT *
FROM (SELECT region, quarter, amount FROM pivot_static) AS s
PIVOT (sum(amount) FOR quarter IN ('Q1' AS q1, 'Q2' AS q2, NULL AS missing))
ORDER BY region;

-- Multiple aggregates require aliases and use value_alias_aggregate_alias names.
SELECT *
FROM (SELECT region, quarter, amount FROM pivot_static)
PIVOT (
    sum(amount) AS total,
    count() AS cnt
    FOR quarter IN ('Q1' AS q1, 'Q2' AS q2)
)
ORDER BY region;

-- Source columns inside an aggregate expression are removed from the implicit GROUP BY.
SELECT *
FROM (SELECT region, quarter, amount FROM pivot_static)
PIVOT (sum(amount * 2) FOR quarter IN ('Q1' AS q1))
ORDER BY region;

-- count(*)/count() needs no measure column; NULL is a real pivot value.
SELECT *
FROM (SELECT region, quarter FROM pivot_static)
PIVOT (count(*) FOR quarter IN ('Q1' AS q1, 'Q2' AS q2, NULL AS missing))
ORDER BY region;

-- PIVOT can follow an unaliased table function. The contextual parser must recover PIVOT
-- from the implicit-alias slot instead of treating it as a table alias.
SELECT *
FROM values('region String, quarter String, amount UInt64', ('east', 'Q1', 1), ('east', 'Q2', 2))
PIVOT (sum(amount) FOR quarter IN ('Q1' AS q1, 'Q2' AS q2));

-- If every input column is consumed by PIVOT, the implicit GROUP BY is empty.
SELECT *
FROM values('k String, v UInt64', ('a', 1), ('b', 2))
PIVOT (sum(v) FOR k IN ('a' AS a, 'b' AS b));

-- Implicit source aliases and their column-alias lists are preserved by the PIVOT wrapper.
SELECT *
FROM numbers(2) n(x)
PIVOT (count() FOR n.x IN (0 AS zero, 1 AS one));

-- Source columns take precedence over same-named outer aliases in aggregate expressions.
WITH 999 AS v
SELECT *
FROM values('k String, v UInt64', ('a', 2))
PIVOT (sum(v) FOR k IN ('a' AS a));

-- Query-scoped aliases that are not source columns remain usable in aggregate expressions.
WITH 2 AS scale
SELECT *
FROM values('k String, v UInt64', ('a', 3))
PIVOT (sum(v * scale) FOR k IN ('a' AS a));

-- Query parameters inside the generated source remain visible to AST visitors.
SET param_pivot_n = 2;
SELECT *
FROM numbers({pivot_n:UInt64})
PIVOT (count() FOR number IN (0 AS zero, 1 AS one));

-- The rewritten AST must format and reparse stably.
WITH $$SELECT * FROM values('k String, v UInt64', ('a', 1)) PIVOT (sum(v) FOR k IN ('a' AS a))$$ AS q
SELECT formatQuerySingleLine(q) = formatQuerySingleLine(formatQuerySingleLine(q));

-- A source alias is visible inside the PIVOT body; a result alias is visible outside it.
SELECT p.region, p.q1
FROM values('region String, quarter String, amount UInt64', ('east', 'Q1', 3)) AS s
PIVOT (sum(s.amount) FOR s.quarter IN ('Q1' AS q1)) AS p;

-- Existing implicit table alias + column-alias-list syntax named `pivot` must not be stolen.
SELECT number FROM numbers(1) pivot(x);

-- Error surfaces.
-- A scalar root must not accidentally become a valid PIVOT aggregate.
SELECT * FROM values('k String, v UInt64', ('a', 1))
PIVOT (if(v > 0, v, 0) FOR k IN ('a' AS a)); -- { serverError UNKNOWN_FUNCTION,UNKNOWN_AGGREGATE_FUNCTION }

-- Compound paths that are not the source qualifier are rejected rather than misclassified as grouping columns.
SELECT * FROM values('k Tuple(x String), v UInt64', (tuple('a'), 1))
PIVOT (sum(v) FOR k.x IN ('a' AS a)); -- { serverError SYNTAX_ERROR }

-- Existing -If aggregates are deliberately rejected in the static parser rewrite.
SELECT * FROM values('k String, v UInt64, ok UInt8', ('a', 1, 1))
PIVOT (sumIf(v, ok) FOR k IN ('a' AS a)); -- { serverError ILLEGAL_AGGREGATION }

SELECT * FROM values('k String, v UInt64', ('a', 1))
PIVOT (sum(v), count() FOR k IN ('a' AS a)); -- { serverError SYNTAX_ERROR }

SELECT * FROM values('k UInt64, v UInt64', (1, 1))
PIVOT (sum(v) FOR k IN ((1 + 0) AS one)); -- { serverError SYNTAX_ERROR }

SELECT * FROM values('k String, v UInt64', ('a', 1))
PIVOT (sum(v) FOR k IN ('a' AS same, 'a' AS same)); -- { serverError SYNTAX_ERROR }

SELECT * FROM values('k String, v UInt64', ('a', 1))
PIVOT (sum(arrayMap(x -> x, [v])) FOR k IN ('a' AS a)); -- { serverError SYNTAX_ERROR }

SELECT * FROM values('k String, v UInt64', ('a', 1))
PIVOT (sum(v) OVER () FOR k IN ('a' AS a)); -- { serverError SYNTAX_ERROR }

DROP TABLE pivot_static;
