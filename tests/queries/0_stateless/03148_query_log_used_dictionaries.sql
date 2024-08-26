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
    allow_experimental_analyzer = 1,
    log_comment = 'simple_with_analyzer'
FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT log_comment, used_dictionaries
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = 'simple_with_analyzer';

SELECT *
FROM (
    SELECT
        dictGet('03148_dictionary', 'name', number) as dict_value
    FROM numbers(1)
) t
SETTINGS
    allow_experimental_analyzer = 1,
    log_comment = 'nested_with_analyzer'
FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT log_comment, used_dictionaries
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = 'nested_with_analyzer';

SELECT
    dictGet('03148_dictionary', 'name', number) as dict_value
FROM numbers(1)
SETTINGS
    allow_experimental_analyzer = 0,
    log_comment = 'simple_without_analyzer'
FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT log_comment, used_dictionaries
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = 'simple_without_analyzer';

SELECT *
FROM (
    SELECT
        dictGet('03148_dictionary', 'name', number) as dict_value
    FROM numbers(1)
) t
SETTINGS
    allow_experimental_analyzer = 0,
    log_comment = 'nested_without_analyzer'
FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT log_comment, used_dictionaries
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = 'nested_without_analyzer';

DROP DICTIONARY IF EXISTS 03148_dictionary;
