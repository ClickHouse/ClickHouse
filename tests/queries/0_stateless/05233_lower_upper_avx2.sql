SELECT concat(
    toString(min(lower(value) = lower_expected)),
    ',',
    toString(min(upper(value) = upper_expected)))
FROM
(
    SELECT
        concat(repeat('A', intDiv(n, 2)), repeat('z', intDiv(n, 2)), if(n % 2, 'Q', '')) AS value,
        concat(repeat('a', intDiv(n, 2)), repeat('z', intDiv(n, 2)), if(n % 2, 'q', '')) AS lower_expected,
        concat(repeat('A', intDiv(n, 2)), repeat('Z', intDiv(n, 2)), if(n % 2, 'Q', '')) AS upper_expected
    FROM (SELECT arrayJoin([0, 1, 15, 16, 17, 31, 32, 33, 64]) AS n)
);

SELECT concat(
    toString(lower(value) = lower_expected),
    ',',
    toString(upper(value) = upper_expected))
FROM
(
    SELECT
        concat(repeat('A', 31), unhex('C385'), repeat('z', 31)) AS value,
        concat(repeat('a', 31), unhex('C385'), repeat('z', 31)) AS lower_expected,
        concat(repeat('A', 31), unhex('C385'), repeat('Z', 31)) AS upper_expected
);

SELECT concat(
    toString(lower(value) = lower_expected),
    ',',
    toString(upper(value) = upper_expected))
FROM
(
    SELECT
        toFixedString(concat('Az', repeat('x', 29), toString(number)), 32) AS value,
        toFixedString(concat('az', repeat('x', 29), toString(number)), 32) AS lower_expected,
        toFixedString(concat('AZ', repeat('X', 29), toString(number)), 32) AS upper_expected
    FROM numbers(1)
);
