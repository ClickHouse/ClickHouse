SELECT
    number,
    toString(number),
    range(number) AS arr,
    arrayMap(x -> toString(x), arr) AS arr_s,
    arrayMap(x -> range(x), arr) AS arr_arr,
    arrayMap(x -> arrayMap(y -> toString(y), x), arr_arr) AS arr_arr_s,
    arrayMap(x -> toFixedString(x, 3), arr_s) AS arr_fs
FROM system.numbers
LIMIT 5, 10;
