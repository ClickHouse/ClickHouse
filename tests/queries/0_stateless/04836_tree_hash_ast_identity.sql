-- The result of a table function is cached for the duration of the query under the tree hash of its
-- AST, so a part of the AST that `getTreeHash` does not see makes the second call reuse the result of
-- the first one. Every query below calls `view` twice with arguments that differ only in such a part.

-- `ASTSelectWithUnionQuery::union_mode`.
SELECT k, s FROM (
    SELECT 1 AS k, count() AS s FROM view(SELECT 1 AS x UNION ALL SELECT 1)
    UNION ALL
    SELECT 2 AS k, count() AS s FROM view(SELECT 1 AS x UNION DISTINCT SELECT 1)
) ORDER BY k;

-- `ASTSelectIntersectExceptQuery::final_operator`.
SELECT k, s FROM (
    SELECT 1 AS k, sum(number) AS s FROM view(SELECT * FROM numbers(4) INTERSECT SELECT * FROM numbers(2))
    UNION ALL
    SELECT 2 AS k, sum(number) AS s FROM view(SELECT * FROM numbers(4) EXCEPT SELECT * FROM numbers(2))
) ORDER BY k;

-- The role of a child of `ASTSelectQuery`: the same literal is the limit in one query and the offset
-- in the other.
SELECT k, s FROM (
    SELECT 1 AS k, sum(number) AS s FROM view(SELECT number FROM numbers(5) LIMIT 2)
    UNION ALL
    SELECT 2 AS k, sum(number) AS s FROM view(SELECT number FROM numbers(5) OFFSET 2)
) ORDER BY k;

-- The role of a child of `ASTOrderByElement`: the same literal is the `WITH FILL` upper bound in one
-- query and the step in the other.
SELECT k, s FROM (
    SELECT 1 AS k, sum(x) AS s FROM view(SELECT number AS x FROM numbers(2) ORDER BY x WITH FILL FROM 0 TO 5)
    UNION ALL
    SELECT 2 AS k, sum(x) AS s FROM view(SELECT number AS x FROM numbers(2) ORDER BY x WITH FILL FROM 0 STEP 5)
) ORDER BY k;

-- The frame of `ASTWindowDefinition`: the same offset bounds the frame from below in one query and
-- from above in the other.
SELECT k, s FROM (
    SELECT 1 AS k, sum(w) AS s FROM view(SELECT sum(number) OVER (ORDER BY number ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS w FROM numbers(4))
    UNION ALL
    SELECT 2 AS k, sum(w) AS s FROM view(SELECT sum(number) OVER (ORDER BY number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS w FROM numbers(4))
) ORDER BY k;

-- The parameters of `ASTColumnsApplyTransformer`, which are not children of it either.
SELECT * FROM (
    SELECT 1 AS k, * FROM view(SELECT COLUMNS('x') APPLY(quantile(0.1)) FROM (SELECT number AS x FROM numbers(11)))
    UNION ALL
    SELECT 2 AS k, * FROM view(SELECT COLUMNS('x') APPLY(quantile(0.9)) FROM (SELECT number AS x FROM numbers(11)))
) ORDER BY k;

-- A per-column PRIMARY KEY is normalized into the storage definition by the parser; the leftover
-- `primary_key_specifier` on the columns must not survive, or the AST would not round-trip
-- format+parse to the same tree hash (which the debug build verifies for every query).
DROP TABLE IF EXISTS t_04836;
CREATE TABLE t_04836 (a UInt8 PRIMARY KEY, b String PRIMARY KEY) ENGINE = MergeTree;
SELECT primary_key FROM system.tables WHERE database = currentDatabase() AND name = 't_04836';
DROP TABLE t_04836;
