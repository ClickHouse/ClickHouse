DROP TABLE IF EXISTS alias_create_query;
DROP TABLE IF EXISTS alias_create_query_target;

CREATE TABLE alias_create_query_target (id UInt64) ENGINE = Memory;
CREATE TABLE alias_create_query ENGINE = Alias('alias_create_query_target');

SELECT create_table_query
FROM system.tables
WHERE database = currentDatabase() AND name = 'alias_create_query';

SHOW CREATE TABLE alias_create_query FORMAT TSVRaw;

DETACH TABLE alias_create_query;
ATTACH TABLE alias_create_query;

ALTER TABLE alias_create_query_target ADD COLUMN value String;

SELECT name, type
FROM system.columns
WHERE database = currentDatabase() AND table = 'alias_create_query'
ORDER BY position;

SELECT create_table_query
FROM system.tables
WHERE database = currentDatabase() AND name = 'alias_create_query';

DROP TABLE alias_create_query;
DROP TABLE alias_create_query_target;
