-- `enable_global_with_statement` copies the first branch's WITH list into every later branch of a UNION,
-- INTERSECT or EXCEPT that has no WITH of its own. The copy used to arrive without `recursive_with`, the flag
-- recording that the list was written WITH RECURSIVE, so the later branch held a recursive CTE while claiming to
-- be non-recursive. Its self-reference then resolved as an ordinary table: absent (UNKNOWN_TABLE), or, when an
-- enclosing CTE of the same name existed, that one instead, which returned a wrong value with no error.
-- CREATE VIEW stored the copied branch without RECURSIVE too, making the loss durable.

SET enable_analyzer = 1;
SET enable_global_with_statement = 1;

-- The recursive CTE used throughout yields ids 1, 2, 3, so sum(id) = 6 and count() = 3. Branches that must
-- agree are given the same aggregate, so the output does not depend on the order the branches emit rows in.

SELECT 'top-level UNION ALL';
WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
SELECT sum(id) AS s FROM src
UNION ALL
SELECT sum(id) AS s FROM src;

-- The copy is not recursive, so `src` inside its body resolves outward to the enclosing CTE, whose
-- 99 fails `id < 3`; the branch was then left with its seed row alone and returned `1` silently.
SELECT 'shadowed by an enclosing plain CTE';
WITH src AS (SELECT 99 AS id)
SELECT * FROM
(
    WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
    SELECT sum(id) AS s FROM src
    UNION ALL
    SELECT sum(id) AS s FROM src
)
ORDER BY s;

SELECT 'CTE column alias list';
WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3)
SELECT sum(n) AS s FROM t
UNION ALL
SELECT sum(n) AS s FROM t;

SELECT 'INTERSECT';
WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
SELECT sum(id) AS s FROM src
INTERSECT
SELECT sum(id) AS s FROM src;

SELECT 'EXCEPT';
WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
SELECT sum(id) AS s FROM src
EXCEPT
SELECT count() AS s FROM src;

SELECT 'three branches';
SELECT * FROM
(
    WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
    SELECT sum(id) AS s FROM src
    UNION ALL
    SELECT sum(id) AS s FROM src
    UNION ALL
    SELECT count() AS s FROM src
)
ORDER BY s;

-- The stored definition is asserted as text rather than by reading the view back: the read-back value
-- additionally depends on ApplyWithSubqueryVisitor, which does not consult `recursive_with` at all.
SELECT 'RECURSIVE survives in a stored view definition';
DROP VIEW IF EXISTS v_05111;
CREATE VIEW v_05111 AS
WITH src AS (SELECT 99 AS id)
SELECT * FROM
(
    WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
    SELECT sum(id) AS s FROM src
    UNION ALL
    SELECT sum(id) AS s FROM src
);
SELECT countSubstrings(create_table_query, 'WITH RECURSIVE') FROM system.tables
WHERE database = currentDatabase() AND name = 'v_05111';
DROP VIEW v_05111;

SELECT 'RECURSIVE survives in the rewritten query';
SELECT countSubstrings(explain, 'WITH RECURSIVE') FROM
(
    EXPLAIN SYNTAX
    WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
    SELECT sum(id) AS s FROM src
    UNION ALL
    SELECT sum(id) AS s FROM src
);

SELECT 'control: non-recursive CTE is propagated as before';
WITH src AS (SELECT 1 AS id)
SELECT sum(id) AS s FROM src
UNION ALL
SELECT sum(id) AS s FROM src;

SELECT 'control: a non-recursive list is propagated without a RECURSIVE marker';
SELECT countSubstrings(explain, 'WITH RECURSIVE') FROM
(
    EXPLAIN SYNTAX
    WITH src AS (SELECT 1 AS id)
    SELECT sum(id) AS s FROM src
    UNION ALL
    SELECT sum(id) AS s FROM src
);

SELECT 'control: WITH RECURSIVE carrying only a scalar alias';
WITH RECURSIVE 1 AS x
SELECT x
UNION ALL
SELECT x;

SELECT 'control: WITH RECURSIVE whose CTE does not reference itself';
WITH RECURSIVE a AS (SELECT 7 AS id)
SELECT sum(id) AS s FROM a
UNION ALL
SELECT sum(id) AS s FROM a;

SELECT 'control: without the propagation the later branch sees no CTE at all';
WITH src AS (SELECT 99 AS id)
SELECT * FROM
(
    WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
    SELECT sum(id) AS s FROM src
    UNION ALL
    SELECT sum(id) AS s FROM src
)
ORDER BY s
SETTINGS enable_global_with_statement = 0; -- { serverError UNKNOWN_TABLE }

-- A branch that has its own WITH receives no CTE from the first branch, with or without the flag: only
-- expression aliases are merged into an existing list, and a CTE is not one.
SELECT 'boundary: the later branch has its own WITH';
SELECT * FROM
(
    WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
    SELECT sum(id) AS s FROM src
    UNION ALL
    WITH other AS (SELECT 0 AS z) SELECT sum(id) AS s FROM src
); -- { serverError UNKNOWN_TABLE }

SELECT 'boundary: a later branch with its own WITH keeps it non-recursive';
SELECT * FROM
(
    WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
    SELECT sum(id) AS s FROM src
    UNION ALL
    WITH other AS (SELECT 5 AS z) SELECT sum(z) AS s FROM other
)
ORDER BY s;
SELECT countSubstrings(explain, 'WITH RECURSIVE') FROM
(
    EXPLAIN SYNTAX
    WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
    SELECT sum(id) AS s FROM src
    UNION ALL
    WITH other AS (SELECT 5 AS z) SELECT sum(z) AS s FROM other
);
