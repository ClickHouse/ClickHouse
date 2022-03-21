SELECT DISTINCT
    eq
FROM
    (
        WITH
            range(2 + number % 10) AS arr, -- minimum two elements, to avoid nan result --
            arrayMap(x -> x = intDiv(number, 10) ? nan : x, arr) AS arr_with_nan,
            arrayFilter(x -> x != intDiv(number, 10), arr) AS arr_filtered
        SELECT
            number,
            arrayReduce('quantileBFloat16', arr_with_nan) AS q1,
            arrayReduce('quantileBFloat16', arr_filtered) AS q2,
            q1 = q2 AS eq
        FROM
            numbers(100)
    );
