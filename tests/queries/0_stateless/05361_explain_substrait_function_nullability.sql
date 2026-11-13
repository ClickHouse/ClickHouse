-- Tags: no-fasttest
-- no-fasttest: Substrait is not built in fast test

SET enable_substrait = 1;

DROP TABLE IF EXISTS t_substrait_nullability;
CREATE TABLE t_substrait_nullability (id Int64, k Int32, n Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_substrait_nullability VALUES (1, 1, 1), (2, 1, NULL);

SELECT 'A keyed sum is nullable in Substrait and not in ClickHouse';
SELECT JSON_QUERY(explain, '$.relations[0].root.input.project.input.aggregate.measures[0].measure.outputType')
FROM (EXPLAIN SUBSTRAIT SELECT sum(id) + 1 FROM t_substrait_nullability GROUP BY k);

SELECT 'A function over it is nullable';
SELECT JSON_QUERY(explain, '$.relations[0].root.input.project.expressions[0].scalarFunction.outputType')
FROM (EXPLAIN SUBSTRAIT SELECT sum(id) + 1 FROM t_substrait_nullability GROUP BY k);

SELECT 'The cast to a narrower ClickHouse result type and the function under it are nullable';
SELECT
    JSON_QUERY(explain, '$.relations[0].root.input.project.expressions[0].cast.type'),
    JSON_QUERY(explain, '$.relations[0].root.input.project.expressions[0].cast.input.scalarFunction.outputType')
FROM (EXPLAIN SUBSTRAIT SELECT sum(id) % 3 FROM t_substrait_nullability GROUP BY k);

SELECT 'A function over a required column is required';
SELECT JSON_QUERY(explain, '$.relations[0].root.input.project.expressions[0].scalarFunction.outputType')
FROM (EXPLAIN SUBSTRAIT SELECT id + 1 FROM t_substrait_nullability);

SELECT 'isNull over a nullable argument is required';
SELECT JSON_QUERY(explain, '$.relations[0].root.input.project.expressions[0].scalarFunction.outputType')
FROM (EXPLAIN SUBSTRAIT SELECT isNull(max(n)) FROM t_substrait_nullability GROUP BY k);

DROP TABLE t_substrait_nullability;
