SELECT
    intDiv(number, 2) AS k,
    sumArgMax(number, number % 20),
    sumArgMax(number, leftPad(toString(number % 20), 5, '0')), -- Pad with 0 to preserve number ordering
    sumArgMax(number, [number % 20, number % 20]),
    sumArgMin(number, number % 20),
    sumArgMin(number, leftPad(toString(number % 20), 5, '0')),
    sumArgMin(number, [number % 20, number % 20]),
FROM
(
    SELECT number
    FROM system.numbers
    LIMIT 65537
)
GROUP BY k
    WITH TOTALS
ORDER BY k ASC
    LIMIT 10
SETTINGS group_by_overflow_mode = 'any', totals_mode = 'before_having', max_rows_to_group_by = 100000;
