-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_function_forms;

CREATE TABLE t_push_subcolumns_function_forms
(
    id UInt32,
    tup Tuple(a UInt32, b String),
    m Map(String, UInt32),
    n Nullable(UInt32)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_function_forms VALUES
    (1, (1, 'one'), {'a': 10, 'b': 20}, 1),
    (2, (2, 'two'), {'c': 30}, NULL);

SELECT 'tupleElement';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT tupleElement(tup, 'a') FROM (SELECT tup FROM t_push_subcolumns_function_forms)
)
WHERE explain LIKE '%Output%';
SELECT tupleElement(tup, 'a') FROM (SELECT tup FROM t_push_subcolumns_function_forms) ORDER BY id;

SELECT 'mapKeys';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT mapKeys(m) FROM (SELECT m FROM t_push_subcolumns_function_forms)
)
WHERE explain LIKE '%Output%';
SELECT mapKeys(m) FROM (SELECT m FROM t_push_subcolumns_function_forms) ORDER BY id;

SELECT 'isNull';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT isNull(n) FROM (SELECT n FROM t_push_subcolumns_function_forms)
)
WHERE explain LIKE '%Output%';
SELECT isNull(n) FROM (SELECT n FROM t_push_subcolumns_function_forms) ORDER BY id;

DROP TABLE t_push_subcolumns_function_forms;
