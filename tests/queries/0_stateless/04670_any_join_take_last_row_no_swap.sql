-- Tags: no-old-analyzer
DROP TABLE IF EXISTS t_any;
DROP TABLE IF EXISTS t_small;

CREATE TABLE t_any (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY (k, v);
INSERT INTO t_any VALUES (1, 2), (1, 3), (2, 4);
CREATE TABLE t_small (k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_small VALUES (1);

-- default (query_plan_join_swap_table = 'auto'): returns 2 — WRONG
SELECT v FROM t_small ANY LEFT JOIN t_any USING (k)
SETTINGS join_algorithm = 'hash', join_any_take_last_row = 1;

-- swap pinned off: returns 3 — correct
SELECT v FROM t_small ANY LEFT JOIN t_any USING (k)
SETTINGS join_algorithm = 'hash', join_any_take_last_row = 1,
         query_plan_join_swap_table = 'false';

-- swap forced on: returns 2 — WRONG
SELECT v FROM t_small ANY LEFT JOIN t_any USING (k)
SETTINGS join_algorithm = 'hash', join_any_take_last_row = 1,
         query_plan_join_swap_table = 'true';
