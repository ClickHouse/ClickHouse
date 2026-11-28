-- The `obfuscate` table function shares its model code with the `clickhouse obfuscator` tool, so it
-- supports exactly the column types that tool supports. Everything else is rejected up front with a
-- clean `NOT_IMPLEMENTED` exception rather than being passed through unobfuscated - the supported-type
-- contract is documented in the table function description and pinned here.

SET obfuscate_seed = 'seed';

SELECT * FROM obfuscate(SELECT toIntervalDay(1) AS x) LIMIT 1; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM obfuscate(SELECT 1::Variant(UInt8, String) AS x) LIMIT 1; -- { serverError NOT_IMPLEMENTED }

-- An unsupported type nested inside a supported wrapper is rejected too.
SELECT * FROM obfuscate(SELECT [toIntervalDay(1)] AS x) LIMIT 1; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM obfuscate(SELECT (1, toIntervalDay(1)) AS x) LIMIT 1; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM obfuscate(SELECT map('a', toIntervalDay(1)) AS x) LIMIT 1; -- { serverError NOT_IMPLEMENTED }

-- The supported types keep working, including through the `Array`/`Nullable` wrappers.
SELECT count() FROM (
    SELECT * FROM obfuscate(
        SELECT
            toInt32(number) AS i,
            toFloat64(number) AS f,
            toDate('2020-01-01') AS d,
            toDateTime('2020-01-01 00:00:00', 'UTC') AS dt,
            toString(number) AS s,
            toFixedString('abc', 3) AS fs,
            generateUUIDv4() AS u,
            [toInt32(number)] AS arr,
            CAST(number, 'Nullable(UInt64)') AS n
        FROM numbers(4)
    ) LIMIT 4
);

-- The pieces preserved exactly: `Date` values pass through unchanged, `Nullable` keeps the original null
-- map, and `Array` keeps the original sizes. The first pass of generation transforms the source rows in
-- order, so the source and the obfuscated rows are compared one by one.
SELECT
    (SELECT groupArray((d, isNull(n), length(arr))) FROM (
        SELECT toDate('2020-01-01') + number AS d, if(number % 2 = 0, NULL, number) AS n, range(number % 3) AS arr FROM numbers(6)))
    =
    (SELECT groupArray((d, isNull(n), length(arr))) FROM (
        SELECT * FROM obfuscate(
            SELECT toDate('2020-01-01') + number AS d, if(number % 2 = 0, NULL, number) AS n, range(number % 3) AS arr FROM numbers(6)
        ) LIMIT 6))
SETTINGS max_threads = 1;
