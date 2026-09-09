-- The `obfuscate` table function shares its model code with the `clickhouse obfuscator` tool, so it
-- supports exactly the column types that tool supports. Everything else is rejected up front with a
-- clean `NOT_IMPLEMENTED` exception rather than being passed through unobfuscated - the supported-type
-- contract is documented in the table function description and pinned here.
--
-- `LowCardinality` has its own test (`04404_obfuscate_unsupported_lowcardinality`) and the wide
-- integers are covered by `04405_obfuscate_integer_type_edge_cases`.

SET obfuscate_seed = 'seed';

-- Modern temporal types: `Date32` and `DateTime64` are distinct types from `Date`/`DateTime` and have
-- no model of their own.
SELECT * FROM obfuscate(SELECT toDate32('2020-01-01') AS d) LIMIT 1; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM obfuscate(SELECT toDateTime64('2020-01-01 00:00:00', 3) AS ts) LIMIT 1; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM obfuscate(SELECT now64() AS ts) LIMIT 1; -- { serverError NOT_IMPLEMENTED }

SELECT * FROM obfuscate(SELECT toDecimal64(1, 2) AS x) LIMIT 1; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM obfuscate(SELECT CAST('a', 'Enum8(\'a\' = 1)') AS x) LIMIT 1; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM obfuscate(SELECT toIPv4('127.0.0.1') AS x) LIMIT 1; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM obfuscate(SELECT toIPv6('::1') AS x) LIMIT 1; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM obfuscate(SELECT (1, 'a') AS x) LIMIT 1; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM obfuscate(SELECT map('a', 1) AS x) LIMIT 1; -- { serverError NOT_IMPLEMENTED }

-- An unsupported type nested inside a supported wrapper is rejected too.
SELECT * FROM obfuscate(SELECT [toDateTime64('2020-01-01 00:00:00', 3)] AS x) LIMIT 1; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM obfuscate(SELECT CAST(NULL, 'Nullable(Date32)') AS x) LIMIT 1; -- { serverError NOT_IMPLEMENTED }

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
