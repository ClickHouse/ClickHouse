SELECT
    number,
    clamp(number, toUInt64(10), toUInt64(20))
FROM numbers(0, 31)
ORDER BY number
FORMAT TabSeparatedRaw;

SELECT
    number,
    clamp(toString(number), '!', '~')
FROM numbers(0, 3)
ORDER BY number
FORMAT TabSeparatedRaw;

SELECT
    number,
    clamp([number], [10], [20])
FROM numbers(0, 31)
ORDER BY number
FORMAT TabSeparatedRaw;

SELECT
    number,
    clamp(toNullable(10), number, number + 20)
FROM numbers(0, 5)
ORDER BY number
FORMAT TabSeparatedRaw;

SELECT
    number,
    clamp(toNullable(number), toNullable(10), toNullable(20))
FROM numbers(5, 18)
ORDER BY number
FORMAT TabSeparatedRaw;

SELECT count()
FROM
(
    SELECT clamp(number, toUInt64(3), toUInt64(2))
    FROM numbers(0)
)
FORMAT TabSeparatedRaw;

SELECT clamp(number, toUInt64(3), toUInt64(2))
FROM numbers(1); -- { serverError BAD_ARGUMENTS }
