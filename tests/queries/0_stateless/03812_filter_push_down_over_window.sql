-- { echoOff }
CREATE TABLE test1
(
    id Int64
)
ENGINE = MergeTree();

CREATE VIEW v1
AS (
    SELECT
        id,
        lagInFrame(id, 1, -1) OVER (
            ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS prev_id
    FROM test1
    ORDER BY id
);

INSERT INTO test1 VALUES (1), (2), (3);

-- { echoOn }
SELECT id, prev_id FROM v1 ORDER BY id;
SELECT id, prev_id FROM v1 WHERE id >= 2 ORDER BY id;
SELECT id, prev_id FROM v1 WHERE prev_id = 1 ORDER BY id;
SELECT id, prev_id FROM v1 WHERE id >= 2 AND prev_id = 1 ORDER BY id;

SELECT id, prev_id FROM v1 ORDER BY id SETTINGS query_plan_filter_push_down_over_window = 1;
SELECT id, prev_id FROM v1 WHERE id >= 2 ORDER BY id SETTINGS query_plan_filter_push_down_over_window = 1;
SELECT id, prev_id FROM v1 WHERE prev_id = 1 ORDER BY id SETTINGS query_plan_filter_push_down_over_window = 1;
SELECT id, prev_id FROM v1 WHERE id >= 2 AND prev_id = 1 ORDER BY id SETTINGS query_plan_filter_push_down_over_window = 1;
-- { echoOff }

CREATE TABLE test2
(
    category String,
    id Int64,
    value Int64
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO test2 VALUES ('x', 1, 100), ('x', 2, 200), ('y', 3, 300), ('y', 4, 400);

CREATE VIEW v2
AS (
    SELECT
        category,
        id,
        value,
        sum(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum
    FROM test2
);

-- { echoOn }
SELECT category, id, value, running_sum FROM v2 ORDER BY id;
SELECT category, id, value, running_sum FROM v2 WHERE category = 'y' ORDER BY id;
SELECT category, id, value, running_sum FROM v2 WHERE category = 'y' ORDER BY id SETTINGS query_plan_filter_push_down_over_window = 1;
-- { echoOff }
