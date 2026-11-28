CREATE TABLE foo (
     open_time Int64,
     open_price Int8,
     close_price Int8
)
ENGINE = MergeTree
ORDER BY open_time;

INSERT INTO foo SELECT number, cityHash64(number) % 256, cityHash64(number * number) % 256 FROM numbers(30);

-- Both interpolate expression are removed
SELECT
    group_id
FROM (
    SELECT
        intDiv(open_time, 10) AS group_id,
        toFloat64(argMin(open_price, open_time)) as open,
        toFloat64(argMax(close_price, open_time)) as close
    FROM
        foo
    GROUP BY
        group_id
    ORDER BY group_id ASC WITH FILL STEP 1 INTERPOLATE (
        open, close
    )
);

-- `close` interpolate expression is removed
SELECT
    group_id, open
FROM (
    SELECT
        intDiv(open_time, 10) AS group_id,
        toFloat64(argMin(open_price, open_time)) as open,
        toFloat64(argMax(close_price, open_time)) as close
    FROM
        foo
    GROUP BY
        group_id
    ORDER BY group_id ASC WITH FILL STEP 1 INTERPOLATE (
        open, close
    )
);

-- Both interpolate expressions are kept
SELECT
    group_id, open, close
FROM (
    SELECT
        intDiv(open_time, 10) AS group_id,
        toFloat64(argMin(open_price, open_time)) as open,
        toFloat64(argMax(close_price, open_time)) as close
    FROM
        foo
    GROUP BY
        group_id
    ORDER BY group_id ASC WITH FILL STEP 1 INTERPOLATE (
        open, close
    )
);
