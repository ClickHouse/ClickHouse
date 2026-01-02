-- Tags: no-random-merge-tree-settings
-- Test: ORDER BY alias uses backing columns in horizontal phase.
DROP TABLE IF EXISTS t_vi_order_by_alias;

SET log_queries = 1;
SET log_profile_events = 1;
SET async_insert = 0;

CREATE TABLE t_vi_order_by_alias
(
    a UInt64,
    b UInt64,
    c UInt64,
    d UInt64,
    x UInt64 ALIAS a + b
)
ENGINE = MergeTree
ORDER BY (a + b, c)
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_order_by_alias VALUES
    (3, 0, 2, 30),
    (1, 1, 1, 20),
    (2, 1, 3, 10),
    (2, 2, 1, 40),
    (1, 0, 2, 50);

SELECT countIf((a + b, c) < lag) FROM
(
    SELECT
        a,
        b,
        c,
        lagInFrame((a + b, c), 1, (a + b, c)) OVER (ORDER BY _part, _part_offset) AS lag
    FROM t_vi_order_by_alias
);

SYSTEM FLUSH LOGS query_log;

SELECT ifNull(max(ProfileEvents['VerticalInsertMergingColumns']), 0)
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query_kind IN ('Insert', 'AsyncInsertFlush')
  AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
  AND position(query, 't_vi_order_by_alias') > 0;

SELECT ifNull(max(ProfileEvents['VerticalInsertGatheringColumns']), 0)
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query_kind IN ('Insert', 'AsyncInsertFlush')
  AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
  AND position(query, 't_vi_order_by_alias') > 0;

DROP TABLE t_vi_order_by_alias;
