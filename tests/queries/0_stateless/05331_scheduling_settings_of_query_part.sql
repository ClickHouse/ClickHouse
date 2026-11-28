-- A part of a query is scheduled under the scheduling settings of the query: its own values of them are
-- not used for scheduling, and a query whose parts set them still runs.

SELECT count() FROM (SELECT 1 SETTINGS workload = 'other') SETTINGS workload = 'default';
SELECT count() FROM (SELECT 1 SETTINGS weight = 2);
SELECT (SELECT 1 SETTINGS workload_priority = 1);
SELECT count() FROM (SELECT 1 SETTINGS weight = DEFAULT) SETTINGS weight = 2;

DROP TABLE IF EXISTS t_sched_part_1;
DROP TABLE IF EXISTS t_sched_part_2;
CREATE TABLE t_sched_part_1 (x UInt64) ENGINE = Memory;
CREATE TABLE t_sched_part_2 (x UInt64) ENGINE = Memory;
INSERT INTO t_sched_part_1 SELECT 1 PARALLEL WITH INSERT INTO t_sched_part_2 SETTINGS workload = 'other', weight = 2 SELECT 2;
SELECT * FROM (SELECT * FROM t_sched_part_1 UNION ALL SELECT * FROM t_sched_part_2) ORDER BY 1;
DROP TABLE t_sched_part_1;
DROP TABLE t_sched_part_2;
