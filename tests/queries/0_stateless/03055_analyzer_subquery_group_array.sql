-- https://github.com/ClickHouse/ClickHouse/issues/23344
SELECT logTrace(repeat('Hello', 100)), ignore(*)
FROM (
    SELECT ignore((SELECT groupArrayState(([number], [number])) FROM numbers(19000)))
)
