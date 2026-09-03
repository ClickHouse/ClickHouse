--https://github.com/ClickHouse/ClickHouse/issues/58906
SELECT
    count(_CAST(NULL, 'Nullable(Nothing)')),
    round(avg(_CAST(NULL, 'Nullable(Nothing)'))) AS k
FROM numbers(256)
    SETTINGS enable_analyzer = 1;
