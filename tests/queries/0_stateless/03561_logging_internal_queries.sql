DROP DICTIONARY IF EXISTS test_logging_internal_queries_dict;
CREATE DICTIONARY test_logging_internal_queries_dict
(
    `name` String,
    `value` Float64
)
PRIMARY KEY name
SOURCE(
    CLICKHOUSE(
        QUERY 'SELECT toString(number) AS name, toFloat64(number) AS value FROM system.numbers LIMIT 10'
    )
)
LIFETIME(30)
LAYOUT(HASHED());

SYSTEM RELOAD DICTIONARY test_logging_internal_queries_dict;

SYSTEM FLUSH LOGS;

SELECT countIf(type = 'QueryStart') > 0, countIf(type = 'QueryFinish') > 0
FROM system.query_log
WHERE is_internal = 1 AND current_database = currentDatabase();

DROP DICTIONARY test_logging_internal_queries_dict;
