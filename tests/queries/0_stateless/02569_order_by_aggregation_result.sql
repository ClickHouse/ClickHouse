-- Github issues:
-- - https://github.com/ClickHouse/ClickHouse/issues/46268
-- - https://github.com/ClickHouse/ClickHouse/issues/46273

-- Queries that the original PR (https://github.com/ClickHouse/ClickHouse/pull/42827) tried to fix
SELECT (number = 1) AND (number = 2) AS value, sum(value) OVER () FROM numbers(1) WHERE 1;
SELECT time, round(exp_smooth, 10), bar(exp_smooth, -9223372036854775807, 1048575, 50) AS bar FROM (SELECT 2 OR (number = 0) OR (number >= 1) AS value, number AS time, exponentialTimeDecayedSum(2147483646)(value, time) OVER (RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS exp_smooth FROM numbers(1) WHERE 10) WHERE 25;

CREATE TABLE ttttttt
(
    `timestamp` DateTime,
    `col1` Float64,
    `col2` Float64,
    `col3` Float64
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO ttttttt VALUES ('2023-02-20 00:00:00', 1, 2, 3);

-- Query that https://github.com/ClickHouse/ClickHouse/pull/42827 broke
SELECT
    argMax(col1, timestamp) AS col1,
    argMax(col2, timestamp) AS col2,
    col1 / col2 AS final_col
FROM ttttttt
GROUP BY
    col3
ORDER BY final_col DESC;

SELECT
    argMax(col1, timestamp) AS col1,
    col1 / 10 AS final_col,
    final_col + 1 AS final_col2
FROM ttttttt
GROUP BY col3;

-- https://github.com/ClickHouse/ClickHouse/issues/46724

CREATE TABLE table1
(
    id String,
    device UUID
)
ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO table1 VALUES ('notEmpty', '417ddc5d-e556-4d27-95dd-a34d84e46a50');
INSERT INTO table1 VALUES ('', '417ddc5d-e556-4d27-95dd-a34d84e46a50');
INSERT INTO table1 VALUES ('', '00000000-0000-0000-0000-000000000000');

SELECT
    if(empty(id), toString(device), id) AS device,
    multiIf(
            notEmpty(id),'a',
            device == '00000000-0000-0000-0000-000000000000', 'b',
            'c' ) AS device_id_type,
    count()
FROM table1
GROUP BY device, device_id_type
ORDER BY device;
