SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
SET mutations_sync = 2;

CREATE TABLE mutation_set_expressions
(
    id UInt64,
    v UInt64,
    w UInt64,
    n Nullable(UInt64),
    label UInt64 ALIAS id + 10
)
ENGINE = Memory;

INSERT INTO mutation_set_expressions
SELECT number, number + 10, number + 100, if(number = 1, NULL, number) FROM numbers(4);

-- Nested sets and table aliases must use the same resolved nodes for set collection
-- and action construction. The scalar subquery and the other assignment see the old row.
ALTER TABLE mutation_set_expressions UPDATE
    v = id IN
        (SELECT k FROM (SELECT number AS k FROM numbers(4)) AS keys
         WHERE k IN (SELECT number FROM numbers(2))),
    w = v + (SELECT toUInt64(5))
WHERE 1;
SELECT id, v, w FROM mutation_set_expressions ORDER BY id;

-- Later commands see earlier updates; the assignment order within one command does not.
ALTER TABLE mutation_set_expressions
    UPDATE v = v + 10 WHERE label IN (SELECT number + 10 FROM numbers(2)),
    UPDATE w = v WHERE id IN (SELECT number FROM numbers(3));
SELECT id, v, w FROM mutation_set_expressions ORDER BY id;

-- A runtime set reading a materialized CTE must retain its gate when optimized,
-- including when its own filter contains another set subquery.
ALTER TABLE mutation_set_expressions UPDATE v = id IN
(
    WITH c AS MATERIALIZED (SELECT number AS k FROM numbers(3))
    SELECT a.k FROM c AS a INNER JOIN c AS b ON a.k = b.k
    WHERE a.k IN (SELECT number + 1 FROM numbers(2))
)
WHERE 1;
SELECT id, v, w FROM mutation_set_expressions ORDER BY id;

-- Preserve nullable set semantics, also when plan optimizations are explicitly disabled.
SET transform_null_in = 1;
SET query_plan_enable_optimizations = 0;
ALTER TABLE mutation_set_expressions UPDATE v = n IN
    (SELECT CAST(NULL AS Nullable(UInt64)) UNION ALL SELECT CAST(2 AS Nullable(UInt64)))
WHERE 1;
SELECT id, v, w FROM mutation_set_expressions ORDER BY id;

DROP TABLE mutation_set_expressions;
