-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/70296
-- INSERT into a source table of a `WINDOW VIEW` used to fail with
-- `NOT_FOUND_COLUMN_IN_BLOCK` ("Not found column  in block") while pushing
-- to the window view. The data was inserted into the main table but the
-- push to the view raised an exception. The bug no longer reproduces on
-- master; this test guards against regressions.

DROP TABLE IF EXISTS my_table;
DROP TABLE IF EXISTS window_table;

SET allow_experimental_window_view = 1;
SET allow_experimental_analyzer = 0;

CREATE TABLE my_table
(
    `num` UInt32,
    `insertion_time` DateTime64(9),
    `simulation_time` DateTime64(9)
)
ENGINE = MergeTree()
ORDER BY (num)
SETTINGS index_granularity = 8192;

CREATE WINDOW VIEW window_table
(
    `num` UInt32,
    `insertion_time` DateTime,
    `simulation_time` DateTime64(9)
)
ENGINE = Memory WATERMARK toIntervalSecond(5)
AS SELECT
    my_table.num AS num,
    tumbleStart(w_id) AS insertion_time,
    max(my_table.simulation_time) AS simulation_time
FROM my_table
WHERE my_table.num != 0
GROUP BY
    my_table.num,
    tumble(CAST(toStartOfSecond(my_table.insertion_time), 'DateTime'), toIntervalSecond(1)) AS w_id
ORDER BY insertion_time ASC;

-- Used to throw NOT_FOUND_COLUMN_IN_BLOCK while pushing to `window_table`.
INSERT INTO my_table
(
    num,
    simulation_time,
    insertion_time
)
VALUES
(1001, toDateTime64('2024-10-01 12:00:00', 9), toDateTime64('2024-10-01 12:30:00', 9));

SELECT num, insertion_time, simulation_time FROM my_table ORDER BY num;

DROP TABLE window_table;
DROP TABLE my_table;
