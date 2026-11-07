-- Tags: no-parallel
-- A part of a query cannot change the scheduling settings of the query through a settings profile either.

DROP SETTINGS PROFILE IF EXISTS s_05332_weight, s_05332_workload, s_05332_other, s_05332_const;
CREATE SETTINGS PROFILE s_05332_weight SETTINGS weight = 2;
CREATE SETTINGS PROFILE s_05332_workload SETTINGS workload = 'other';
CREATE SETTINGS PROFILE s_05332_other SETTINGS max_threads = 1;

SELECT * FROM (SELECT getSetting('weight') SETTINGS profile = 's_05332_weight'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM (SELECT 1 SETTINGS profile = 's_05332_workload'); -- { serverError BAD_ARGUMENTS }

-- A profile that does not change scheduling settings is allowed.
SELECT * FROM (SELECT 1 SETTINGS profile = 's_05332_other');

DROP TABLE IF EXISTS t_05332_1;
DROP TABLE IF EXISTS t_05332_2;
CREATE TABLE t_05332_1 (x UInt64) ENGINE = Memory;
CREATE TABLE t_05332_2 (x UInt64) ENGINE = Memory;
INSERT INTO t_05332_1 SELECT 1 PARALLEL WITH INSERT INTO t_05332_2 SETTINGS profile = 's_05332_workload' SELECT 2; -- { serverError BAD_ARGUMENTS }
INSERT INTO t_05332_1 SELECT 1 PARALLEL WITH INSERT INTO t_05332_2 SETTINGS profile = 's_05332_other' SELECT 2;
SELECT * FROM t_05332_1 UNION ALL SELECT * FROM t_05332_2 ORDER BY 1;
DROP TABLE t_05332_1;
DROP TABLE t_05332_2;

-- A part whose setting is clamped by a constraint to the query's value is allowed, also for a reset.
DROP SETTINGS PROFILE IF EXISTS s_05332_const;
CREATE SETTINGS PROFILE s_05332_const SETTINGS weight = 2 CONST;
SET profile = 's_05332_const';
SELECT * FROM (SELECT getSetting('weight') SETTINGS weight = 1);
SELECT * FROM (SELECT getSetting('weight') SETTINGS weight = DEFAULT);

DROP SETTINGS PROFILE s_05332_weight, s_05332_workload, s_05332_other, s_05332_const;
