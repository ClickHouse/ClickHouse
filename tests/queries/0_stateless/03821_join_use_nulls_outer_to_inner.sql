#!/usr/bin/env -S ${HOME}/clickhouse-client --queries-file

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t2 (id UInt64, val UInt64) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t3 (id UInt64, val2 Nullable(Int64)) ENGINE = MergeTree() ORDER BY id;

INSERT INTO t1 VALUES (4000, 'b'), (5000, 'a');
INSERT INTO t2 VALUES (5000, 1), (5000, 2), (6000, 3);
INSERT INTO t3 VALUES (5000, 1), (5000, 2), (5000, 3), (7000, 10);

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET join_use_nulls = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET optimize_move_to_prewhere = 1;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val) || '_' || toString(r2.val2 + 1) || toTypeName(r2.val2)
FROM t1 AS l
LEFT JOIN t2 AS r ON l.id = r.id
LEFT JOIN t3 AS r2 ON l.id = r2.id
WHERE val > 1
ORDER BY 1;


SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val) || '_' || toString(r2.val2 + 1) || toTypeName(r2.val2)
    FROM t1 AS l
    LEFT JOIN t2 AS r ON l.id = r.id
    LEFT JOIN t3 AS r2 ON l.id = r2.id
    WHERE val > 1
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL') OR trim(explain) LIKE '%Prewhere filter column%';

SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val) || '_' || toString(r2.val2 + 1) || toTypeName(r2.val2)
FROM t1 AS l
FULL JOIN t2 AS r ON l.id = r.id
LEFT JOIN t3 AS r2 ON l.id = r2.id
WHERE val IS NOT NULL
ORDER BY 1;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val) || '_' || toString(r2.val2 + 1) || toTypeName(r2.val2)
    FROM t1 AS l
    FULL JOIN t2 AS r ON l.id = r.id
    LEFT JOIN t3 AS r2 ON l.id = r2.id
    WHERE val IS NOT NULL
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL') OR trim(explain) LIKE '%Prewhere filter column%';

SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val) || '_' || toString(r2.val2 + 1) || toTypeName(r2.val2)
FROM t1 AS l
FULL JOIN t2 AS r ON l.id = r.id
LEFT JOIN t3 AS r2 ON l.id = r2.id
WHERE val IS NOT NULL AND val2 == 3
ORDER BY 1;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val) || '_' || toString(r2.val2 + 1) || toTypeName(r2.val2)
    FROM t1 AS l
    FULL JOIN t2 AS r ON l.id = r.id
    LEFT JOIN t3 AS r2 ON l.id = r2.id
    WHERE val IS NOT NULL AND val2 == 3
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL') OR trim(explain) LIKE '%Prewhere filter column%';


SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val) || '_' || toString(r2.val2 + 1) || toTypeName(r2.val2)
FROM t1 AS l
FULL JOIN t2 AS r ON l.id = r.id
LEFT JOIN t3 AS r2 ON l.id = r2.id
WHERE val IS NULL OR val > 1
ORDER BY 1;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val) || '_' || toString(r2.val2 + 1) || toTypeName(r2.val2)
    FROM t1 AS l
    FULL JOIN t2 AS r ON l.id = r.id
    LEFT JOIN t3 AS r2 ON l.id = r2.id
    WHERE val IS NULL OR val > 1
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL') OR trim(explain) LIKE '%Prewhere filter column%';


-- Combination of OUTER to INNER and equivalent sets optimization:

SELECT count() FROM t1 as l
LEFT JOIN t2 AS r ON l.id = r.id
WHERE l.id = 5000 AND r.id > 0;

SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val)
FROM t1 AS l
LEFT JOIN t2 AS r ON l.id = r.id
WHERE l.id = 5000 AND r.id > 0;

SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val)
FROM t1 AS l
LEFT JOIN t2 AS r ON l.id+1 = r.id+1
WHERE l.id+1 = 5001 AND r.id+1 > 0;

SELECT
    if(countIf(explain LIKE '%Prewhere filter %equals(__table1.id, 5000_UInt16)%') >= 1
        -- Condition to t2.id == 5000 pushed down to right side by equvalent sets optimization.
        -- Column name there may have internal prefix because of implementation detail
        -- that helps to distringuish original non nullale column from nullable column created by join_use_nulls.
       AND countIf(explain LIKE '%Prewhere filter %equals(%__table2.id, 5000_UInt16)%') >= 1
       -- t.id > 0 uses applied after JOIN, so it uses nullable column.
       AND countIf(explain LIKE '%Prewhere filter %greater(__table2.id, 0_UInt8)%') >= 1,
      'OK', 'Error: \n' || arrayStringConcat(groupArray(explain), '\n'))
FROM (
    EXPLAIN PLAN actions = 1
    SELECT l.name || '_' || toString(r.val + 1) || toTypeName(r.val)
    FROM t1 AS l
    LEFT JOIN t2 AS r ON l.id = r.id
    WHERE l.id = 5000 AND r.id > 0
);
