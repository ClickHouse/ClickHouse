DROP DICTIONARY IF EXISTS 03148_dictionary;

CREATE DICTIONARY 03148_dictionary (
    id UInt64,
    name String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    QUERY 'select 0 as id, ''name0'' as name'
))
LIFETIME(MIN 1 MAX 10)
LAYOUT(HASHED);

SELECT
    dictGet('03148_dictionary', 'name', number) as dict_value
FROM numbers(1)
SETTINGS
    enable_analyzer = 1,
    log_comment = 'simple_with_analyzer'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, used_dictionaries
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = 'simple_with_analyzer'
  AND is_internal = 0;

SELECT *
FROM (
    SELECT
        dictGet('03148_dictionary', 'name', number) as dict_value
    FROM numbers(1)
) t
SETTINGS
    enable_analyzer = 1,
    log_comment = 'nested_with_analyzer'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, used_dictionaries
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = 'nested_with_analyzer'
  AND is_internal = 0;

SELECT
    dictGet('03148_dictionary', 'name', number) as dict_value
FROM numbers(1)
SETTINGS
    enable_analyzer = 0,
    log_comment = 'simple_without_analyzer'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, used_dictionaries
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = 'simple_without_analyzer'
  AND is_internal = 0;

SELECT *
FROM (
    SELECT
        dictGet('03148_dictionary', 'name', number) as dict_value
    FROM numbers(1)
) t
SETTINGS
    enable_analyzer = 0,
    log_comment = 'nested_without_analyzer'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, used_dictionaries
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = 'nested_without_analyzer'
  AND is_internal = 0;

DROP DICTIONARY IF EXISTS 03148_dictionary;
