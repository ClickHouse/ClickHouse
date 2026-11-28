-- `ParserShowTablesQuery` accepts `FULL` before every form. Debug builds format, reparse and compare every query,
-- so the formatter must keep it.
SELECT formatQuerySingleLine(q) FROM (SELECT arrayJoin([
    'SHOW FULL DATABASES NOT LIKE ''x'' LIMIT 1',
    'SHOW FULL CLUSTERS ILIKE ''x''',
    'SHOW FULL CLUSTER test_shard_localhost',
    'SHOW FULL FILESYSTEM CACHES',
    'SHOW FULL SETTINGS LIKE ''x''',
    'SHOW FULL CHANGED SETTINGS ILIKE ''x''',
    'SHOW FULL MERGES LIKE ''x'' LIMIT 1',
    'SHOW FULL TEMPORARY TABLES',
    'SHOW FULL DICTIONARIES FROM db LIKE ''x''']) AS q) ORDER BY q;

SHOW FULL DATABASES LIMIT 0;
SHOW FULL CLUSTERS LIMIT 0;
SHOW FULL CLUSTER test_shard_localhost FORMAT Null;
SHOW FULL FILESYSTEM CACHES FORMAT Null;
SHOW FULL SETTINGS LIKE 'max_threads' FORMAT Null;
SHOW FULL CHANGED SETTINGS LIKE 'max_threads' FORMAT Null;
SHOW FULL MERGES LIMIT 0;

-- `FULL` has no effect for dictionaries.
CREATE TABLE src (id UInt64) ENGINE = Memory;
CREATE DICTIONARY dict (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src')) LAYOUT(FLAT()) LIFETIME(0);
SHOW FULL DICTIONARIES;
