-- `IN` with `transform_null_in = 0` skips the `accurateOrNull` cast when the probe type equals the set key type.
SET transform_null_in = 0;

SELECT number, number IN (SELECT number * 2 FROM numbers(5)) FROM numbers(6);

-- `Nullable` probe against a `Nullable` key: a NULL probe gives NULL, not a match with the NULL in the set.
SELECT x, x IN (SELECT arrayJoin([1, NULL]::Array(Nullable(UInt64)))) FROM (SELECT arrayJoin([0, 1, NULL]::Array(Nullable(UInt64))) AS x);

-- Two key columns: one with an identical type, the other cast to `Nullable`, whose NULLs must still be non-members.
SELECT a, b, (a, b) IN (SELECT number, number FROM numbers(3)) FROM (SELECT number AS a, if(number = 1, NULL, number) AS b FROM numbers(4));

-- `Bool` equals `UInt8` but is not an identity cast: the cast normalizes nonzero values to `true`.
SELECT x, x IN (SELECT true), x NOT IN (SELECT true) FROM (SELECT arrayJoin([0, 1, 2, 255]::Array(UInt8)) AS x);
