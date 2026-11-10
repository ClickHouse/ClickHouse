SELECT
    uniqUpTo(0)(toString(number)) AS threshold_0,
    uniqUpTo(1)(toString(number % 1)) AS one_distinct,
    uniqUpTo(1)(toString(number % 2)) AS one_plus_one,
    uniqUpTo(10)(toString(number % 10)) AS threshold_10,
    uniqUpTo(10)(toString(number % 11)) AS threshold_10_plus_one,
    uniqUpTo(100)(toString(number % 100)) AS threshold_100,
    uniqUpTo(100)(toString(number % 101)) AS threshold_100_plus_one,
    uniqUpTo(1)(toFixedString(toString(number % 2), 8)) AS fixed_string_value,
    uniqUpTo(1)(toUInt128(number % 2)) AS uint128_value,
    uniqUpTo(1)(toUInt256(number % 2)) AS uint256_value,
    uniqUpTo(1)(toInt256(number % 2) - 1) AS int256_value,
    uniqUpTo(1)(toUUID(concat('00000000-0000-0000-0000-00000000000', toString(number % 2)))) AS uuid_value,
    uniqUpTo(10)((toString(number % 10), number % 2)) AS tuple_value,
    uniqUpTo(1)((toString(number % 2), number % 2)) AS saturated_tuple_value,
    uniqUpTo(1)([toString(number % 2)]) AS array_value,
    uniqUpTo(1)(([toString(number % 2)], toString(number % 2))) AS tuple_array_value,
    uniqUpTo(1)(toString(number % 2), number % 2) AS variadic_value
FROM numbers(1000);

SELECT uniqUpTo(1)(value)
FROM
(
    SELECT if(number = 0, NULL, toString(number % 2)) AS value
    FROM numbers(3)
);

SELECT uniqUpToMerge(10)(state)
FROM
(
    SELECT uniqUpToState(10)(toString(number % 11)) AS state
    FROM numbers(1000)
    GROUP BY number % 3
);
