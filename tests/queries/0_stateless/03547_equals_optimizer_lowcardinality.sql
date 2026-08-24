DROP TABLE IF EXISTS test;

CREATE TABLE test (d1 Dynamic(max_types=2), d2 Dynamic(max_types=2)) ENGINE = Memory;

INSERT INTO test VALUES (42, 42), (42, 43), (43, 42), ('abc', 'abc'), ('abc', 'abd'), ('abd', 'abc'),
([1,2,3], [1,2,3]), ([1,2,3], [1,2,4]), ([1,2,4], [1,2,3]),
('2020-01-01', '2020-01-01'), ('2020-01-01', '2020-01-02'), ('2020-01-02', '2020-01-01'),
(NULL, NULL), (42, 'abc'), ('abc', 42), (42, [1,2,3]), ([1,2,3], 42), (42, NULL), (NULL, 42),
('abc', [1,2,3]), ([1,2,3], 'abc'), ('abc', NULL), (NULL, 'abc'), ([1,2,3], NULL), (NULL, [1,2,3]),
(42, '2020-01-01'), ('2020-01-01', 42), ('2020-01-01', 'abc'), ('abc', '2020-01-01'),
('2020-01-01', [1,2,3]), ([1,2,3], '2020-01-01'), ('2020-01-01', NULL), (NULL, '2020-01-01');

SELECT
    dynamicType(d2)
FROM test
GROUP BY
    dynamicType(d2)
HAVING
    0
    OR (
        (
            materialize(toLowCardinality(0)) = 0
        ) = anyLast(1)
    )
ORDER BY 1;

SELECT
    dynamicType(d2)
FROM test
GROUP BY
    dynamicType(d2)
HAVING
    0
    OR (
        (
            materialize(toLowCardinality(0)) = 0
        ) = anyLast(0)
    )
ORDER BY 1;

DROP TABLE IF EXISTS test;
