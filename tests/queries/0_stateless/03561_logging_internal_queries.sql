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

SYSTEM FLUSH LOGS query_log;

SELECT countIf(type = 'QueryStart') > 0, countIf(type = 'QueryFinish') > 0
FROM system.query_log
WHERE 1
    AND is_internal = 1
    AND query = 'SELECT toString(number) AS name, toFloat64(number) AS value FROM system.numbers LIMIT 10'
    -- internal dictionary queries always use the dictionary source's context
    -- so, they are not aware of the session and system.query_log.current_database is always 'default'
    -- but the style check demands that the tests that select from the query log have 'current_database.*currentDatabase()'
    -- hence, this plaster is needed
    AND current_database IN ['default', currentDatabase()];

DROP DICTIONARY test_logging_internal_queries_dict;
