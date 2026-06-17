-- Verify topKThroughJoin is skipped where it would change semantics: SEMI/ANTI
-- joins (preserved-side rows can be filtered out) and LIMIT WITH TOTALS.

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1;

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;

CREATE TABLE t_l (id UInt64, k Int64) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t_r (id UInt64) ENGINE = MergeTree() ORDER BY id;

INSERT INTO t_l SELECT number, -toInt64(number) FROM numbers(100);
INSERT INTO t_r SELECT number * 2 + 1 FROM numbers(50);

SELECT 'left_semi' AS label, count(*), max(k), min(k) FROM (
    SELECT l.id, l.k FROM t_l AS l LEFT SEMI JOIN t_r AS r ON r.id = l.id
    ORDER BY l.k DESC LIMIT 10
);

SELECT 'left_anti' AS label, count(*), max(k), min(k) FROM (
    SELECT l.id, l.k FROM t_l AS l LEFT ANTI JOIN t_r AS r ON r.id = l.id
    ORDER BY l.k DESC LIMIT 10
);

SELECT 'right_semi' AS label, count(*) FROM (
    SELECT r.id FROM t_l AS l RIGHT SEMI JOIN t_r AS r ON r.id = l.id
    ORDER BY r.id DESC LIMIT 10
);

SELECT 'right_anti' AS label, count(*) FROM (
    SELECT r.id FROM t_l AS l RIGHT ANTI JOIN t_r AS r ON r.id = l.id
    ORDER BY r.id DESC LIMIT 10
);

-- `LEFT SEMI` plan must have exactly one Sort + Limit pair, the outer one.
SELECT 'left_semi_plan_sort_count' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.id, l.k FROM t_l AS l LEFT SEMI JOIN t_r AS r ON r.id = l.id
    ORDER BY l.k DESC LIMIT 10
    SETTINGS enable_parallel_replicas = 0
);

-- WITH TOTALS sets `alwaysReadTillEnd` on the LimitStep; the optimization must be
-- skipped so totals are computed over the full join output.
SELECT 'limit_with_totals_plan_sort_count' AS label, countIf(explain LIKE '%Sorting%') AS sort_count
FROM ( EXPLAIN actions = 0
    SELECT l.id, l.k, count() AS c FROM t_l AS l LEFT JOIN t_r AS r ON r.id = l.id
    GROUP BY l.id, l.k WITH TOTALS
    ORDER BY l.k DESC LIMIT 10
    SETTINGS enable_parallel_replicas = 0
);

-- Alias shadowing: the visible `k` is a computed expression (`l.k + r.bonus`),
-- so `ORDER BY k` cannot be pushed onto the preserved input.
DROP TABLE IF EXISTS t_lb;
DROP TABLE IF EXISTS t_rb;
CREATE TABLE t_lb (id UInt64, k Int64) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t_rb (id UInt64, bonus Int64) ENGINE = MergeTree() ORDER BY id;
INSERT INTO t_lb VALUES (1, 100), (2, 90);
INSERT INTO t_rb VALUES (1, 0), (2, 1000);

SELECT 'alias_shadow_correctness' AS label, id, k FROM (
    SELECT l.id AS id, l.k + r.bonus AS k
    FROM t_lb AS l LEFT JOIN t_rb AS r ON r.id = l.id
    ORDER BY k DESC LIMIT 1
);

DROP TABLE t_lb;
DROP TABLE t_rb;
DROP TABLE t_l;
DROP TABLE t_r;
