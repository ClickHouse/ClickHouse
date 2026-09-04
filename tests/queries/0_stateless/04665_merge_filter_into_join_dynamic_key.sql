-- Tags: no-random-merge-tree-settings

-- A `WHERE` equality on a `Dynamic` column is not merged into the JOIN condition
-- unless `allow_dynamic_type_in_join_keys` is enabled.

DROP TABLE IF EXISTS t_int;
DROP TABLE IF EXISTS t_dyn;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_merge_filter_into_join_condition = 1;
SET query_plan_join_swap_table = 'false';
SET enable_join_runtime_filters = 0;
-- CI randomizes this to 0, which leaves `ON 1` reported as `inner` instead of `cross`.
SET query_plan_optimize_join_order_limit = 10;

CREATE TABLE t_int (a Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_dyn (d Dynamic) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_int VALUES (1), (2), (3);
INSERT INTO t_dyn VALUES (1), (2), (5);

SELECT '-- `Dynamic` key is rejected with allow_dynamic_type_in_join_keys = 0';
SELECT
    extract(arrayStringConcat(groupArray(explain), '\n'), 'Type: (\\w+)') AS join_kind,
    extract(arrayStringConcat(groupArray(explain), '\n'), 'Join conditions: ([^\n]*)') AS join_conditions,
    countIf(explain LIKE '%Filter column:%') AS filters_above_join
FROM (
    EXPLAIN SELECT * FROM (SELECT * FROM t_int INNER JOIN t_dyn ON 1) WHERE a = d
    SETTINGS allow_dynamic_type_in_join_keys = 0
);

SELECT a, toString(d) FROM (SELECT * FROM t_int INNER JOIN t_dyn ON 1) WHERE a = d
ORDER BY ALL SETTINGS allow_dynamic_type_in_join_keys = 0;

SELECT '-- `Dynamic` key is merged with allow_dynamic_type_in_join_keys = 1';
SELECT
    extract(arrayStringConcat(groupArray(explain), '\n'), 'Type: (\\w+)') AS join_kind,
    extract(arrayStringConcat(groupArray(explain), '\n'), 'Join conditions: ([^\n]*)') AS join_conditions,
    countIf(explain LIKE '%Filter column:%') AS filters_above_join
FROM (
    EXPLAIN SELECT * FROM (SELECT * FROM t_int INNER JOIN t_dyn ON 1) WHERE a = d
    SETTINGS allow_dynamic_type_in_join_keys = 1
);

DROP TABLE t_int;
DROP TABLE t_dyn;
