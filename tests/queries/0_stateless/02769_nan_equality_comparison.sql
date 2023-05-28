SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

SELECT nan AS value, value = value, value = materialize(value), materialize(value) = value, materialize(value) = materialize(value);
SELECT cast(nan, 'Float32') AS value, value = value, value = materialize(value), materialize(value) = value, materialize(value) = materialize(value);
SELECT nan AS lhs, cast(nan, 'Float32') AS rhs, lhs = rhs, lhs = materialize(rhs), materialize(lhs) = rhs, materialize(lhs) = materialize(rhs);

SELECT '--';

CREATE TABLE test_table
(
    id UInt32,
    value UInt32
) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_table VALUES (76, 57);

SELECT value FROM (SELECT stddevSamp(id) AS value FROM test_table) as subquery
WHERE ((value = value) AND (NOT (value = value)));

DROP TABLE test_table;
