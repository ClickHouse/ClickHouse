CREATE TABLE src (id UInt64, val String) ENGINE = Memory;
INSERT INTO src VALUES (1, 'a');

CREATE DICTIONARY dict_eager (id UInt64, val String) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
SETTINGS(dictionary_lazy_load = 0);

CREATE DICTIONARY dict_lazy (id UInt64, val String) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
SETTINGS(dictionary_lazy_load = 1);

CREATE DICTIONARY dict_auto (id UInt64, val String) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
SETTINGS(dictionary_lazy_load = 'auto');

SELECT name, status FROM system.dictionaries WHERE database = currentDatabase() ORDER BY name;

CREATE DICTIONARY dict_bad (id UInt64, val String) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'no_such_table'))
LAYOUT(HASHED())
LIFETIME(0)
SETTINGS(dictionary_lazy_load = 0); -- { serverError UNKNOWN_TABLE }
