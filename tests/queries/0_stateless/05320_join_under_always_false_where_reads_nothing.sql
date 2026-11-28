-- A WHERE that no row passes is pushed to every JOIN input, including one that push-down is
-- disabled for, so the second UNION ALL branch does not read `t_big` (issue #123702).

SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS t_live;
DROP TABLE IF EXISTS t_small;
DROP TABLE IF EXISTS t_big;
DROP TABLE IF EXISTS t_hint_left;
DROP TABLE IF EXISTS t_hint_right;

CREATE TABLE t_live (d Date, c String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_small (d Date, c String) ENGINE = MergeTree ORDER BY d;
CREATE TABLE t_big (c String, v UInt64) ENGINE = MergeTree ORDER BY c;

INSERT INTO t_live SELECT toDate('2026-01-01') + number, toString(number % 10) FROM numbers(100);
INSERT INTO t_small SELECT toDate('2026-01-01') + number, toString(number % 10) FROM numbers(100);
INSERT INTO t_big SELECT toString(number % 10), number FROM numbers(100000);

-- The outer WHERE is false for the second UNION ALL branch, whose constant `k` it reads.
SELECT 'LEFT JOIN', count()
FROM (SELECT d, 'b' AS k FROM t_live UNION ALL SELECT s.d AS d, 'x' AS k FROM t_small AS s LEFT JOIN t_big AS b ON s.c = b.c)
WHERE k = 'b' AND d >= '2026-02-01'
SETTINGS query_plan_join_swap_table = 'false', log_comment = '05320_left';

SELECT 'INNER JOIN', count()
FROM (SELECT d, 'b' AS k FROM t_live UNION ALL SELECT s.d AS d, 'x' AS k FROM t_small AS s INNER JOIN t_big AS b ON s.c = b.c)
WHERE k = 'b' AND d >= '2026-02-01'
SETTINGS query_plan_join_swap_table = 'false', log_comment = '05320_inner';

SELECT 'LEFT JOIN, aggregated right side', count()
FROM (SELECT d, 'b' AS k FROM t_live UNION ALL SELECT s.d AS d, 'x' AS k FROM t_small AS s LEFT JOIN (SELECT c, max(v) AS v FROM t_big GROUP BY c) AS b ON s.c = b.c)
WHERE k = 'b' AND d >= '2026-02-01'
SETTINGS query_plan_join_swap_table = 'false', log_comment = '05320_left_aggregated';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, read_rows < 10000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05320_left', '05320_inner', '05320_left_aggregated')
ORDER BY log_comment;

-- `indexHint` reads no column and is true: it must stay off the non-preserved side, where it would prune rows.
CREATE TABLE t_hint_left (id UInt32) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;
CREATE TABLE t_hint_right (id UInt32, v Int64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;
INSERT INTO t_hint_left SELECT number FROM numbers(1000);
INSERT INTO t_hint_right SELECT number, number + 1000 FROM numbers(1000);

SELECT 'indexHint on the non-preserved side', count()
FROM t_hint_left AS l LEFT JOIN t_hint_right AS r ON l.id = r.id
WHERE indexHint(r.id < 150) AND (r.id < 150)
SETTINGS use_join_disjunctions_push_down = 0, query_plan_join_swap_table = 'false';

DROP TABLE t_live;
DROP TABLE t_small;
DROP TABLE t_big;
DROP TABLE t_hint_left;
DROP TABLE t_hint_right;
