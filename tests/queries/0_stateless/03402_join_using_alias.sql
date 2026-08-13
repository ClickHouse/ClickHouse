set enable_analyzer=1;

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Int, c1 Int ALIAS 1) ENGINE = Memory;
SELECT c0 FROM remote('localhost', currentDatabase(), 't0') tx JOIN t0 USING (c1); -- { serverError BAD_ARGUMENTS }

SELECT c0 FROM remote('localhost', currentDatabase(), 't0') tx JOIN t0 USING (c1) SETTINGS query_plan_use_new_logical_join_step=0; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t0;
