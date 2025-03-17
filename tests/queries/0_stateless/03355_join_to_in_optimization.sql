SET enable_analyzer = 1;
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (`id` Int32, key String, key2 String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (`id` Int32, key String, key2 String) ENGINE = MergeTree ORDER BY id;
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

EXPLAIN
SELECT hostName() AS hostName
FROM system.query_log AS a
INNER JOIN system.processes AS b ON (a.query_id = b.query_id) AND (type = 'QueryStart')
WHERE current_database = currentDatabase()
SETTINGS query_plan_use_new_logical_join_step = true, query_plan_convert_join_to_in = true;