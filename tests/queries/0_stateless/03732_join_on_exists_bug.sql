DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Int) ENGINE = Memory;
INSERT INTO t0 VALUES (1), (2), (3);

SELECT 1 FROM t0 ASOF JOIN t0 tx ON EXISTS (SELECT 1) JOIN t0 ty ON t0.c0 = ty.c0
; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT 1 FROM t0 ASOF JOIN t0 tx ON EXISTS (SELECT 1) JOIN t0 ty ON t0.c0 = ty.c0
SETTINGS allow_general_join_planning = 0; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT 1 FROM t0 ASOF JOIN t0 tx ON EXISTS (SELECT 1) JOIN t0 ty ON t0.c0 = ty.c0
SETTINGS query_plan_use_new_logical_join_step = 0; -- { serverError INVALID_JOIN_ON_EXPRESSION }
