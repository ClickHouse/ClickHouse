-- https://github.com/ClickHouse/ClickHouse/issues/23344
SET enable_analyzer=1;
SELECT logTrace(repeat('Hello', 100)), ignore(*)
FROM (
    SELECT ignore((SELECT groupArrayState(([number], [number])) FROM numbers(19000)))
)
