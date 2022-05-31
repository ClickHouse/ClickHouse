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
