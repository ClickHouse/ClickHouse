set enable_analyzer = 1;
set allow_experimental_correlated_subqueries = 1;

SELECT (SELECT count() FROM system.one WHERE number = 2) FROM numbers(2);

SELECT (SELECT count() FROM system.one WHERE number = 2) FROM numbers(2) GROUP BY number % 2; -- { serverError NOT_IMPLEMENTED }

CREATE TABLE A
(
    id UInt32
)
ENGINE = Memory();

INSERT INTO A SELECT number + 1 as id FROM numbers(19);

CREATE TABLE B
(
    id UInt32,
    A_ids Array(UInt32)
)
ENGINE = Memory();

INSERT INTO B (id, A_ids) VALUES
(101, [1, 3, 5]),
(102, [2, 4]),
(103, [1, 7, 9, 11]),
(104, [6, 8, 10]),
(105, [3, 12, 15]),
(106, [16, 18, 20]);


-- The Query using Subqueries
SELECT
    A.id AS a_id,
    (
        SELECT groupArraySorted(5)(B.id)
        FROM B
        WHERE has(B.A_ids, A.id)
    ) AS b_ids_containing_a_id
FROM A
ORDER BY a_id
LIMIT 20;
