-- Calendar-invalid dates (a day number larger than the length of the month) have always been normalized
-- by the lookup table inside `[1900, 2299]`: `makeDate32(1999, 2, 30)` is `1999-03-02`, not an error.
-- After extending `Date32` to `[0000-01-01, 9999-12-31]`, the `cctz` escape path used outside the lookup
-- table must behave exactly the same way, otherwise the same expression would mean different things
-- depending on the year. This test pins that equivalence.

SELECT 'toDate32';
SELECT y, toDate32OrNull(concat(leftPad(toString(y), 4, '0'), '-02-30')) AS feb30, toDate32OrNull(concat(leftPad(toString(y), 4, '0'), '-04-31')) AS apr31
FROM (SELECT arrayJoin([1, 999, 1899, 1999, 2000, 2299, 2300, 2999, 9998]) AS y)
ORDER BY y;

SELECT 'makeDate32';
SELECT y, makeDate32(y, 2, 30) AS feb30, makeDate32(y, 4, 31) AS apr31
FROM (SELECT arrayJoin([1, 999, 1899, 1999, 2000, 2299, 2300, 2999, 9998]) AS y)
ORDER BY y;

SELECT 'YYYYMMDDToDate32';
SELECT y, YYYYMMDDToDate32(y * 10000 + 230) AS feb30, YYYYMMDDToDate32(y * 10000 + 431) AS apr31
FROM (SELECT arrayJoin([1, 999, 1899, 1999, 2000, 2299, 2300, 2999, 9998]) AS y)
ORDER BY y;

-- The normalization is the same shift for every year: February 30 of a common year is March 2,
-- and of a leap year is March 1, regardless of whether the year is inside the lookup table.
SELECT 'leap years';
SELECT y, makeDate32(y, 2, 30)
FROM (SELECT arrayJoin([1600, 1700, 1896, 1996, 2400, 8000]) AS y)
ORDER BY y;

-- Truly malformed components (month or day out of the 1..12 / 1..31 range) still take the error path
-- both inside and outside the lookup table.
SELECT 'malformed components';
SELECT y, makeDate32(y, 0, 1), makeDate32(y, 13, 1), makeDate32(y, 1, 0), makeDate32(y, 1, 32)
FROM (SELECT arrayJoin([1899, 1999, 2999]) AS y)
ORDER BY y;
