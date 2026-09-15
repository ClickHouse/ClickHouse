-- A real subcolumn whose top-level name resembles a planner table alias must not be
-- mistaken for the storage key column after removing a synthetic `__tableN.` qualifier.
DROP TABLE IF EXISTS lag_in_frame_table_qualifier;
CREATE TABLE lag_in_frame_table_qualifier
(
    a UInt64,
    b UInt64,
    ts UInt64,
    value UInt64,
    __table1 Nested(ts UInt64)
)
ENGINE = MergeTree
ORDER BY (a, ts);

INSERT INTO lag_in_frame_table_qualifier VALUES
    (1, 1, 1, 10, [2]),
    (1, 2, 2, 20, [1]),
    (1, 1, 3, 30, [1]),
    (1, 2, 4, 40, [2]);

SELECT countIf(explain LIKE '%StreamingLag%')
FROM
(
    EXPLAIN PIPELINE
    SELECT lagInFrame(value) OVER (PARTITION BY a, b ORDER BY __table1.ts)
    FROM lag_in_frame_table_qualifier
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

DROP TABLE lag_in_frame_table_qualifier;
