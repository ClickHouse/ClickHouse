WITH
    18 AS precision,
    toUInt256(-1) AS int,
    toUInt256(toFloat64(int)) AS converted,
    toString(int) AS int_str,
    toString(converted) AS converted_str
SELECT
    length(int_str) = length(converted_str) AS have_same_length,
    substring(int_str, 1, precision) = substring(converted_str, 1, precision) AS have_same_prefix
