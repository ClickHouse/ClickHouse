-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_function_forms;

CREATE TABLE t_push_subcolumns_function_forms
(
    id UInt32,
    arr Array(UInt32),
    tup Tuple(a UInt32, b String),
    m Map(String, UInt32),
    n Nullable(UInt32),
    v Variant(UInt32, String)
)
ENGINE = MergeTree ORDER BY id;

SET allow_experimental_variant_type = 1;

INSERT INTO t_push_subcolumns_function_forms VALUES
    (1, [1, 2], (1, 'one'), {'a': 10, 'b': 20}, 1, 1),
    (2, [3], (2, 'two'), {'c': 30}, NULL, 'two');

SELECT 'length';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT length(arr) FROM (SELECT arr FROM t_push_subcolumns_function_forms)
)
WHERE explain LIKE '%Output%';
SELECT length(arr) FROM (SELECT id, arr FROM t_push_subcolumns_function_forms) ORDER BY id;

SELECT 'tupleElement';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT tupleElement(tup, 'a') FROM (SELECT tup FROM t_push_subcolumns_function_forms)
)
WHERE explain LIKE '%Output%';
SELECT tupleElement(tup, 'a') FROM (SELECT id, tup FROM t_push_subcolumns_function_forms) ORDER BY id;

SELECT 'mapKeys';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT mapKeys(m) FROM (SELECT m FROM t_push_subcolumns_function_forms)
)
WHERE explain LIKE '%Output%';
SELECT mapKeys(m) FROM (SELECT id, m FROM t_push_subcolumns_function_forms) ORDER BY id;

SELECT 'isNull';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT isNull(n) FROM (SELECT n FROM t_push_subcolumns_function_forms)
)
WHERE explain LIKE '%Output%';
SELECT isNull(n) FROM (SELECT id, n FROM t_push_subcolumns_function_forms) ORDER BY id;

SELECT 'variantElement';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT variantElement(v, 'UInt32') FROM (SELECT v FROM t_push_subcolumns_function_forms)
)
WHERE explain LIKE '%Output%';
SELECT variantElement(v, 'UInt32') FROM (SELECT id, v FROM t_push_subcolumns_function_forms) ORDER BY id;

DROP TABLE t_push_subcolumns_function_forms;
