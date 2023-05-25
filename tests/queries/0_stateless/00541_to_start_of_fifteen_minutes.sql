SELECT
    DISTINCT result 
FROM (
    SELECT
        toStartOfFifteenMinutes(toDateTime('2017-12-25 00:00:00') + number * 60) AS result
    FROM system.numbers
    LIMIT 120
) ORDER BY result
