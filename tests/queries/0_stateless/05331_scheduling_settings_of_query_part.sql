-- A part of a query cannot change the workload or a scheduling setting of the query.

SELECT count() FROM (SELECT 1 SETTINGS workload = 'other') SETTINGS workload = 'default'; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM (SELECT 1 SETTINGS weight = 2); -- { serverError BAD_ARGUMENTS }
SELECT (SELECT 1 SETTINGS workload_priority = 1); -- { serverError BAD_ARGUMENTS }
SELECT 1 WHERE 1 IN (SELECT 1 SETTINGS weight_lowering_factor = 0.5); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM (SELECT 1 SETTINGS weight = DEFAULT) SETTINGS weight = 2; -- { serverError BAD_ARGUMENTS }

-- The same values as the query's are allowed.
SELECT count() FROM (SELECT 1 SETTINGS workload = 'default') SETTINGS workload = 'default';
SELECT count() FROM (SELECT 1 SETTINGS weight = 1);
SELECT count() FROM (SELECT 1 SETTINGS weight = DEFAULT);

-- `PARALLEL WITH` subqueries are parts of the query too.
DROP TABLE IF EXISTS t_sched_part_1;
DROP TABLE IF EXISTS t_sched_part_2;
CREATE TABLE t_sched_part_1 (x UInt64) ENGINE = Memory;
CREATE TABLE t_sched_part_2 (x UInt64) ENGINE = Memory;
INSERT INTO t_sched_part_1 SELECT 1 PARALLEL WITH INSERT INTO t_sched_part_2 SETTINGS workload = 'other' SELECT 2; -- { serverError BAD_ARGUMENTS }
-- Resetting a scheduling setting of a part to its default differs from the query's value too.
SET weight = 2;
INSERT INTO t_sched_part_1 SELECT 1 PARALLEL WITH INSERT INTO t_sched_part_2 SETTINGS weight = DEFAULT SELECT 2; -- { serverError BAD_ARGUMENTS }
SET weight = DEFAULT;
-- The other subquery of a rejected query may have inserted its row before the error.
TRUNCATE TABLE t_sched_part_1;
TRUNCATE TABLE t_sched_part_2;
INSERT INTO t_sched_part_1 SELECT 1 PARALLEL WITH INSERT INTO t_sched_part_2 SELECT 2;
SELECT * FROM t_sched_part_1 UNION ALL SELECT * FROM t_sched_part_2 ORDER BY 1;

-- Query settings in the storage `SETTINGS` clause of a `CREATE` apply to the query too.
DROP TABLE IF EXISTS t_sched_part_3;
DROP TABLE IF EXISTS t_sched_part_4;
CREATE TABLE t_sched_part_3 (x UInt8) ENGINE = Memory SETTINGS weight = 2 PARALLEL WITH CREATE TABLE t_sched_part_4 (x UInt8) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
-- The other subquery may have created its table before the error.
DROP TABLE IF EXISTS t_sched_part_4;
CREATE TABLE t_sched_part_3 (x UInt8) ENGINE = Memory SETTINGS workload = 'other' PARALLEL WITH CREATE TABLE t_sched_part_4 (x UInt8) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
DROP TABLE IF EXISTS t_sched_part_3;
DROP TABLE IF EXISTS t_sched_part_4;
CREATE TABLE t_sched_part_3 (x UInt8) ENGINE = Memory SETTINGS max_threads = 1 PARALLEL WITH CREATE TABLE t_sched_part_4 (x UInt8) ENGINE = Memory;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN ('t_sched_part_3', 't_sched_part_4');
DROP TABLE t_sched_part_3;
DROP TABLE t_sched_part_4;
DROP TABLE t_sched_part_1;
DROP TABLE t_sched_part_2;
