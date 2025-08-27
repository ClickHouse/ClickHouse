SET enable_analyzer = 1;
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (`id` Int32, key String, key2 String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity=8192;
CREATE TABLE t2 (`id` Int32, key String, key2 String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity=8192;
INSERT INTO t1 VALUES (1, '111', '111'),(2, '222', '2'),(2, '222', '222'),(3, '333', '333');
INSERT INTO t2 VALUES (2, 'AAA', 'AAA'),(2, 'AAA', 'a'),(3, 'BBB', 'BBB'),(4, 'CCC', 'CCC');

EXPLAIN actions = 1, optimize = 1, header = 1
SELECT t1.id
FROM t1, t2
WHERE t1.id = t2.id
SETTINGS query_plan_use_new_logical_join_step = true, query_plan_convert_join_to_in = true;

SELECT
    t1.key,
    t1.key2
FROM t1
ALL INNER JOIN t2 ON (t1.id = t2.id) AND (t2.key = t2.key2)
ORDER BY
    t1.key ASC,
    t1.key2 ASC
SETTINGS query_plan_use_new_logical_join_step = true, query_plan_convert_join_to_in = true;

SYSTEM FLUSH LOGS system.query_log;
EXPLAIN
SELECT hostName() AS hostName
FROM system.query_log AS a
INNER JOIN system.processes AS b ON (a.query_id = b.query_id) AND (type = 'QueryStart')
WHERE current_database = currentDatabase()
SETTINGS query_plan_use_new_logical_join_step = true, query_plan_convert_join_to_in = true;

SELECT dummy
FROM
(
    SELECT dummy
    FROM system.one
) AS a
INNER JOIN
(
    SELECT dummy
    FROM system.one
) AS b USING (dummy)
INNER JOIN
(
    SELECT dummy
    FROM system.one
) AS c USING (dummy)
SETTINGS query_plan_use_new_logical_join_step = true, query_plan_convert_join_to_in = true;

-- check type, modified from 02988_join_using_prewhere_pushdown
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (`id` UInt16, `u` LowCardinality(Int32), `s` LowCardinality(String))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t VALUES (1,1,'a'),(2,2,'b');

SELECT
    u,
    s
FROM t
INNER JOIN
(
    SELECT CAST(number, 'Int32') AS u
    FROM numbers(10)
) AS t1 USING (u)
FORMAT Null
SETTINGS query_plan_use_new_logical_join_step = true, query_plan_convert_join_to_in = true;

-- check filter column remove, modified from 01852_multiple_joins_with_union_join
DROP TABLE IF EXISTS v1;
DROP TABLE IF EXISTS v2;

CREATE TABLE v1 ( id Int32 ) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE v2 ( value Int32 ) ENGINE = MergeTree() ORDER BY value;

INSERT INTO v1 ( id ) VALUES (1);
INSERT INTO v2 ( value ) VALUES (1);

SELECT * FROM v1 AS t1
JOIN v1 AS t2 USING (id)
CROSS JOIN v2 AS n1;

-- from fuzzer
SELECT 10
FROM system.query_log AS a
INNER JOIN system.processes AS b
ON (a.query_id = b.query_id) AND (a.query_id = b.query_id)
WHERE current_database = currentDatabase()
FORMAT Null
SETTINGS query_plan_use_new_logical_join_step = true, query_plan_convert_join_to_in = true;
