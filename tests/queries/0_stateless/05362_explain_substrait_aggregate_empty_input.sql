-- Tags: no-fasttest
-- no-fasttest: Substrait is not built in fast test

SET enable_substrait = 1;

DROP TABLE IF EXISTS t_substrait_empty_input;
CREATE TABLE t_substrait_empty_input (id Int64, x Int32, n Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_substrait_empty_input VALUES (1, 1, 1), (2, 2, NULL);

SELECT 'Over empty input ClickHouse returns a default for a plain argument and NULL for a Nullable one';
SELECT sum(x), sum(n), min(n) FROM t_substrait_empty_input WHERE x < 0;

SELECT 'sum of a plain argument without GROUP BY is sum0, which returns 0';
SELECT
    JSON_QUERY(explain, '$.extensions[0].extensionFunction.name'),
    JSON_QUERY(explain, '$.relations[0].root.input.aggregate.measures[0].measure.outputType')
FROM (EXPLAIN SUBSTRAIT SELECT sum(x) FROM t_substrait_empty_input);

SELECT 'sum of a Nullable argument without GROUP BY is sum, which returns NULL';
SELECT
    JSON_QUERY(explain, '$.extensions[0].extensionFunction.name'),
    JSON_QUERY(explain, '$.relations[0].root.input.aggregate.measures[0].measure.outputType')
FROM (EXPLAIN SUBSTRAIT SELECT sum(n) FROM t_substrait_empty_input);

SELECT 'min of a Nullable argument without GROUP BY is min, which returns NULL';
SELECT
    JSON_QUERY(explain, '$.extensions[0].extensionFunction.name'),
    JSON_QUERY(explain, '$.relations[0].root.input.aggregate.measures[0].measure.outputType')
FROM (EXPLAIN SUBSTRAIT SELECT min(n) FROM t_substrait_empty_input);

SELECT 'min of a plain argument without GROUP BY blocks, Substrait has no min that returns 0';
-- With the filter the query cannot be answered from the min-max statistics, so the plan has the aggregation.
EXPLAIN SUBSTRAIT SELECT min(x) FROM t_substrait_empty_input WHERE x > 0; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_substrait_empty_input;
