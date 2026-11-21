-- Several constant arguments use the generic implementation.
SELECT
    number,
    least(number, toUInt64(10), toUInt64(20), toUInt64(30)) AS least_value,
    greatest(number, toUInt64(10), toUInt64(20), toUInt64(30)) AS greatest_value
FROM numbers(32)
ORDER BY number
FORMAT TabSeparatedRaw;

-- The all-dynamic path remains unchanged.
SELECT
    least(materialize(number), materialize(number + 1), materialize(number + 2)) AS least_value,
    greatest(materialize(number), materialize(number + 1), materialize(number + 2)) AS greatest_value
FROM numbers(3)
ORDER BY number
FORMAT TabSeparatedRaw;

-- Generic String comparisons keep constant arguments as constants.
SELECT
    number,
    least(toString(number), '10', '20') AS least_value,
    greatest(toString(number), '10', '20') AS greatest_value
FROM numbers(3)
ORDER BY number
FORMAT TabSeparatedRaw;

-- Generic Array comparisons use the same constant-column path.
SELECT
    number,
    least([number], [0], [2]) AS least_value,
    greatest([number], [0], [2]) AS greatest_value
FROM numbers(3)
ORDER BY number
FORMAT TabSeparatedRaw;

-- Nullable columns and NULL arguments keep their existing behavior.
SELECT
    number,
    least(toNullable(number), toNullable(10), toNullable(20)) AS least_value,
    greatest(toNullable(number), toNullable(10), toNullable(20)) AS greatest_value
FROM numbers(3)
ORDER BY number
FORMAT TabSeparatedRaw;

SELECT
    number,
    least(toNullable(number), NULL, 10) AS least_value,
    greatest(toNullable(number), NULL, 10) AS greatest_value
FROM numbers(3)
ORDER BY number
FORMAT TabSeparatedRaw;
