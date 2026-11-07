SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
SET query_plan_enable_optimizations = 1;
SET query_plan_convert_join_to_in = 1;
SET join_algorithm = 'hash';
SET use_index_for_in_with_subqueries = 1;
SET optimize_move_to_prewhere = 0;

CREATE TABLE mutation_cte_keys (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO mutation_cte_keys VALUES (0), (1), (2);
CREATE TABLE mutation_cte_target (k UInt64, v UInt8) ENGINE = Memory;
INSERT INTO mutation_cte_target VALUES (0, 0), (1, 0), (2, 0);

-- Converting this join creates a set whose source reads `c`. Primary-key analysis
-- must not build it before the outer mutation plan materializes the CTE. Referencing
-- `c` twice keeps it materialized; duplicate keys do not affect the enclosing `IN`.
ALTER TABLE mutation_cte_target UPDATE v = k IN
(
    WITH c AS MATERIALIZED (SELECT number AS k FROM numbers(2))
    SELECT l.k FROM mutation_cte_keys AS l
    ALL INNER JOIN (SELECT k FROM c UNION ALL SELECT k FROM c) AS r ON l.k = r.k
)
WHERE 1;
SELECT k, v FROM mutation_cte_target ORDER BY k;

-- The CTE dependency can live in a deferred set source rather than a child node
-- of the join. Follow two nested sets as well as the direct reader above.
CREATE TABLE mutation_cte_rhs (k UInt64) ENGINE = Memory;
INSERT INTO mutation_cte_rhs VALUES (0), (1), (2);
ALTER TABLE mutation_cte_target UPDATE v = k IN
(
    WITH c AS MATERIALIZED (SELECT number AS k FROM numbers(2))
    SELECT l.k FROM mutation_cte_keys AS l
    ALL INNER JOIN
    (
        SELECT k FROM mutation_cte_rhs
        WHERE k IN
        (
            SELECT k FROM mutation_cte_rhs
            WHERE k IN (SELECT k FROM c UNION ALL SELECT k FROM c)
        )
    ) AS r ON l.k = r.k
)
WHERE 1;
SELECT k, v FROM mutation_cte_target ORDER BY k;

-- The unconverted join is the result control.
SET query_plan_convert_join_to_in = 0;
ALTER TABLE mutation_cte_target UPDATE v = k IN
(
    WITH c AS MATERIALIZED (SELECT number AS k FROM numbers(2))
    SELECT l.k FROM mutation_cte_keys AS l
    ALL INNER JOIN (SELECT k FROM c UNION ALL SELECT k FROM c) AS r ON l.k = r.k
)
WHERE 1;
SELECT k, v FROM mutation_cte_target ORDER BY k;

-- Keep the conversion available for an ordinary `Memory` table with no CTE dependency.
SET query_plan_convert_join_to_in = 1;
CREATE TABLE mutation_cte_ready (k UInt64) ENGINE = Memory;
INSERT INTO mutation_cte_ready VALUES (0), (1);
ALTER TABLE mutation_cte_target UPDATE v = k IN
(
    SELECT l.k FROM mutation_cte_keys AS l
    ANY INNER JOIN mutation_cte_ready AS r ON l.k = r.k
)
WHERE 1;
SELECT k, v FROM mutation_cte_target ORDER BY k;

SET explain_query_plan_default = 'legacy';
SELECT countIf(trimLeft(explain) LIKE 'Join%') = 0, countIf(explain LIKE '%CreatingSets%') > 0
FROM
(
    EXPLAIN description = 0
    SELECT l.k FROM mutation_cte_keys AS l
    ANY INNER JOIN mutation_cte_ready AS r ON l.k = r.k
);

DROP TABLE mutation_cte_target;
DROP TABLE mutation_cte_keys;
DROP TABLE mutation_cte_ready;
DROP TABLE mutation_cte_rhs;
