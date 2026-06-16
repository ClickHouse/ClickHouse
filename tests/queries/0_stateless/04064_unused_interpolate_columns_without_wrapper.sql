-- Tags: no-parallel-replicas
-- ^ because EXPLAIN PLAN output contains ReadFromRemoteParallelReplicas instead of ReadFromMergeTree under parallel replicas.
-- https://github.com/ClickHouse/ClickHouse/issues/75186
-- RemoveUnusedProjectionColumnsPass should remove unused INTERPOLATE columns
-- even when the interpolated expressions are bare aggregate functions (no wrapper like toFloat64).
-- This is the same test as 03322_unused_interpolate_expressions but without toFloat64 wrapping.
-- We verify using EXPLAIN PLAN that unused columns are actually pruned from the Filling step header.

SET enable_analyzer = 1;

CREATE TABLE foo (
     open_time Int64,
     open_price Int8,
     close_price Int8
)
ENGINE = MergeTree
ORDER BY open_time;

INSERT INTO foo SELECT number, cityHash64(number) % 256, cityHash64(number * number) % 256 FROM numbers(30);

-- Both interpolate expressions are removed: Filling header should only have group_id
EXPLAIN PLAN header = 1
SELECT
    group_id
FROM (
    SELECT
        intDiv(open_time, 10) AS group_id,
        argMin(open_price, open_time) AS open,
        argMax(close_price, open_time) AS close
    FROM
        foo
    GROUP BY
        group_id
    ORDER BY group_id ASC WITH FILL STEP 1 INTERPOLATE (
        open, close
    )
);

SELECT '---';

-- `close` interpolate expression is removed: Filling header should have open but not close
EXPLAIN PLAN header = 1
SELECT
    group_id, open
FROM (
    SELECT
        intDiv(open_time, 10) AS group_id,
        argMin(open_price, open_time) AS open,
        argMax(close_price, open_time) AS close
    FROM
        foo
    GROUP BY
        group_id
    ORDER BY group_id ASC WITH FILL STEP 1 INTERPOLATE (
        open, close
    )
);

SELECT '---';

-- Both interpolate expressions are kept: Filling header should have both open and close
EXPLAIN PLAN header = 1
SELECT
    group_id, open, close
FROM (
    SELECT
        intDiv(open_time, 10) AS group_id,
        argMin(open_price, open_time) AS open,
        argMax(close_price, open_time) AS close
    FROM
        foo
    GROUP BY
        group_id
    ORDER BY group_id ASC WITH FILL STEP 1 INTERPOLATE (
        open, close
    )
);
