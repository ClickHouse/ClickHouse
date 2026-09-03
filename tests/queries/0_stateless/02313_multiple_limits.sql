SELECT sum(x)
FROM
(
    SELECT x
    FROM
    (
        SELECT number AS x
        FROM system.numbers
        SETTINGS max_rows_to_read = 10, read_overflow_mode = 'break', max_block_size = 2
    )
    SETTINGS max_rows_to_read = 20, read_overflow_mode = 'break', max_block_size = 2
);

SELECT sum(x)
FROM
(
    SELECT x
    FROM
    (
        SELECT number AS x
        FROM system.numbers
        SETTINGS max_rows_to_read = 20, read_overflow_mode = 'break', max_block_size = 2
    )
    SETTINGS max_rows_to_read = 10, read_overflow_mode = 'break', max_block_size = 2
);


SELECT count() >= 20, count() <= 22
FROM
(
    SELECT x
    FROM
    (
        SELECT zero AS x
        FROM system.zeros
        SETTINGS max_block_size = 2, max_rows_to_read = 10, read_overflow_mode = 'break'
    )
    UNION ALL
        SELECT x
    FROM
    (
        SELECT zero + 1 AS x
        FROM system.zeros
        SETTINGS max_block_size = 2, max_rows_to_read = 20, read_overflow_mode = 'break'
    )
);

SELECT sum(x) >= 10
FROM
(
    SELECT x
    FROM
    (
        SELECT zero AS x
        FROM system.zeros
        SETTINGS max_block_size = 2, max_rows_to_read = 10, read_overflow_mode = 'break'
    )
    UNION ALL
        SELECT x
    FROM
    (
        SELECT zero + 1 AS x
        FROM system.zeros
        SETTINGS max_block_size = 2, max_rows_to_read = 20, read_overflow_mode = 'break'
    )
);

SELECT count() >= 20, count() <= 22
FROM
(
    SELECT x
    FROM
    (
        SELECT zero AS x
        FROM system.zeros
        SETTINGS max_block_size = 2, max_rows_to_read = 20, read_overflow_mode = 'break'
    )
    UNION ALL
        SELECT x
    FROM
    (
        SELECT zero + 1 AS x
        FROM system.zeros
        SETTINGS max_block_size = 2, max_rows_to_read = 10, read_overflow_mode = 'break'
    )
);

SELECT sum(x) <= 10
FROM
(
    SELECT x
    FROM
    (
        SELECT zero AS x
        FROM system.zeros
        SETTINGS max_block_size = 2, max_rows_to_read = 20, read_overflow_mode = 'break'
    )
    UNION ALL
        SELECT x
    FROM
    (
        SELECT zero + 1 AS x
        FROM system.zeros
        SETTINGS max_block_size = 2, max_rows_to_read = 10, read_overflow_mode = 'break'
    )
);
