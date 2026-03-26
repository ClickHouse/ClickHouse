CREATE TABLE test
(
    id Int64
)
ENGINE = MergeTree();

CREATE VIEW v
AS (
    SELECT
        id,
        lagInFrame(id, 1, -1) OVER (
            ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS prev_id
    FROM test
    ORDER BY id
);

INSERT INTO test VALUES (1), (2), (3);

SELECT id, prev_id FROM v ORDER BY id;

SELECT id, prev_id FROM v WHERE id = 3 ORDER BY id;

SELECT id, prev_id FROM v ORDER BY id SETTINGS query_plan_filter_push_down_over_window = 1;

SELECT id, prev_id FROM v WHERE id = 3 ORDER BY id SETTINGS query_plan_filter_push_down_over_window = 1;
