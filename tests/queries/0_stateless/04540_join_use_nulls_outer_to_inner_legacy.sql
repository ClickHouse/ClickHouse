-- The outer-to-inner join conversion for join_use_nulls only runs on the analyzer
-- (JoinStepLogical) path. With enable_analyzer = 0 the legacy JoinStep path
-- (tryConvertOuterJoinToInnerJoinLegacy) bails out for join_use_nulls, so the join
-- kind must stay outer. This guards that the legacy path is a safe no-op fallback
-- and keeps returning correct results, independent of the analyzer optimization.

SET enable_analyzer = 0;
SET join_use_nulls = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS t_o2i_legacy_l;
DROP TABLE IF EXISTS t_o2i_legacy_r;

CREATE TABLE t_o2i_legacy_l (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY (k, id);
CREATE TABLE t_o2i_legacy_r (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY (k, id);

INSERT INTO t_o2i_legacy_l SELECT number, number % 15 FROM numbers(1000);
INSERT INTO t_o2i_legacy_r SELECT number, 5 + (number % 15) FROM numbers(1000);

-- Legacy path keeps the outer kind (no conversion) for all three outer kinds.
SELECT extract(explain, 'Type: (\w+) \| Strictness') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_o2i_legacy_l AS l
    FULL JOIN t_o2i_legacy_r AS r ON l.k = r.k
    WHERE l.k = 10 AND r.k = 10
) WHERE explain LIKE '%Strictness%';

SELECT extract(explain, 'Type: (\w+) \| Strictness') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_o2i_legacy_l AS l
    LEFT JOIN t_o2i_legacy_r AS r ON l.k = r.k
    WHERE r.k = 10
) WHERE explain LIKE '%Strictness%';

SELECT extract(explain, 'Type: (\w+) \| Strictness') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_o2i_legacy_l AS l
    RIGHT JOIN t_o2i_legacy_r AS r ON l.k = r.k
    WHERE l.k = 10
) WHERE explain LIKE '%Strictness%';

-- Results are identical with the optimization on and off on the legacy path.
SELECT count(), sum(l.id), sum(r.id) FROM t_o2i_legacy_l AS l
FULL JOIN t_o2i_legacy_r AS r ON l.k = r.k
WHERE l.k = 10 AND r.k = 10
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count(), sum(l.id), sum(r.id) FROM t_o2i_legacy_l AS l
FULL JOIN t_o2i_legacy_r AS r ON l.k = r.k
WHERE l.k = 10 AND r.k = 10
SETTINGS query_plan_convert_outer_join_to_inner_join = 1;

DROP TABLE t_o2i_legacy_l;
DROP TABLE t_o2i_legacy_r;
