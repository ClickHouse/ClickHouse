SELECT
    'Nul\0able\0String)Nul\0\0ble(String)Nul\0able(String)Nul\0able(String)',
    NULL AND 2,
    '',
    number,
    NULL AS k
FROM
(
    SELECT
        materialize(NULL) OR materialize(-9223372036854775808),
        number
    FROM system.numbers
    LIMIT 1000000
)
ORDER BY
    k ASC,
    number ASC,
    k ASC
LIMIT 1023, 1023
SETTINGS max_bytes_before_external_sort = 1000000, max_bytes_ratio_before_external_sort = 0
FORMAT Null;
