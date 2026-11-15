-- Test that filter push-down over JOINs preserves correct output values
-- when the WHERE predicate expression is also used in the SELECT list.
-- Previously, the optimizer replaced the shared DAG node with constant 1,
-- corrupting the output.  For Float64 predicates this also caused BAD_GET.

DROP TABLE IF EXISTS left_t;
DROP TABLE IF EXISTS right_t;

CREATE TABLE left_t (c0 Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE right_t (c0 Int32) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO left_t VALUES (0), (1), (-1), (2), (100);
INSERT INTO right_t VALUES (1), (2), (3);

-- Float64 predicate (sin) in both WHERE and SELECT with INNER JOIN
SELECT DISTINCT sin(left_t.c0) AS v
FROM left_t INNER JOIN left_t AS r ON left_t.c0 = r.c0
WHERE sin(left_t.c0)
ORDER BY v;

-- Int32 column used as both predicate and output
SELECT left_t.c0
FROM left_t INNER JOIN left_t AS r ON left_t.c0 = r.c0
WHERE left_t.c0
ORDER BY left_t.c0;

-- Float64 predicate with RIGHT OUTER JOIN (tests isFilterAlwaysFalseForDefaultValueInputs too)
SELECT left_t.c0
FROM left_t RIGHT OUTER JOIN right_t ON left_t.c0 = right_t.c0
WHERE radians(left_t.c0)
ORDER BY left_t.c0;

-- Float64 predicate with LEFT OUTER JOIN
SELECT DISTINCT sin(left_t.c0) AS v
FROM left_t LEFT OUTER JOIN right_t ON left_t.c0 = right_t.c0
WHERE sin(left_t.c0)
ORDER BY v;

-- TLP (Ternary Logic Partitioning) correctness:
-- WHERE p UNION ALL WHERE NOT p UNION ALL WHERE p IS NULL
-- must produce the same result set as the unfiltered query.
SELECT DISTINCT * FROM (
    SELECT DISTINCT sin(left_t.c0) FROM left_t INNER JOIN left_t AS r ON left_t.c0 = r.c0 WHERE sin(left_t.c0)
    UNION ALL
    SELECT DISTINCT sin(left_t.c0) FROM left_t INNER JOIN left_t AS r ON left_t.c0 = r.c0 WHERE NOT sin(left_t.c0)
    UNION ALL
    SELECT DISTINCT sin(left_t.c0) FROM left_t INNER JOIN left_t AS r ON left_t.c0 = r.c0 WHERE sin(left_t.c0) IS NULL
) ORDER BY 1;

-- sign() returns Int8, verify it works in outer join context
SELECT left_t.c0
FROM left_t RIGHT OUTER JOIN right_t ON left_t.c0 = right_t.c0
WHERE sign(left_t.c0)
ORDER BY left_t.c0;

-- FULL OUTER JOIN with Float64 filter
SELECT left_t.c0
FROM left_t FULL OUTER JOIN right_t ON left_t.c0 = right_t.c0
WHERE sin(left_t.c0)
ORDER BY left_t.c0;

DROP TABLE left_t;
DROP TABLE right_t;
