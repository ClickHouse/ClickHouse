SELECT
    uniqUpTo(0)(toString(number)) AS threshold_0,
    uniqUpTo(1)(toString(number % 1)) AS one_distinct,
    uniqUpTo(1)(toString(number % 2)) AS one_plus_one,
    uniqUpTo(10)(toString(number % 10)) AS threshold_10,
    uniqUpTo(10)(toString(number % 11)) AS threshold_10_plus_one,
    uniqUpTo(100)(toString(number % 100)) AS threshold_100,
    uniqUpTo(10)((toString(number % 10), number % 2)) AS tuple_value,
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
