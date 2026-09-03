SELECT x
FROM
(
    SELECT if((number % NULL) = -2147483648, NULL, if(toInt64(toInt64(now64(if((number % NULL) = -2147483648, NULL, if(toInt64(now64(toInt64(9223372036854775807, now64(plus(NULL, NULL))), plus(NULL, NULL))) = (number % NULL), nan, toFloat64(number))), toInt64(9223372036854775807, toInt64(9223372036854775807, now64(plus(NULL, NULL))), now64(plus(NULL, NULL))), plus(NULL, NULL))), now64(toInt64(9223372036854775807, toInt64(0, now64(plus(NULL, NULL))), now64(plus(NULL, NULL))), plus(NULL, NULL))) = (number % NULL), nan, toFloat64(number))) AS x
    FROM system.numbers
    LIMIT 3
)
ORDER BY x DESC NULLS LAST
