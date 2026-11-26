-- Tags: no-parallel, no-flaky-check

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dict_data (key UInt64, val UInt64) Engine=Memory();
CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.dict
(
  key UInt64 DEFAULT 0,
  val UInt64 DEFAULT 10
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dict_data' PASSWORD '' DB currentDatabase()))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT());

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.dict_data VALUES(1,11);

SELECT query_count FROM system.dictionaries WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'dict';
SELECT dictGetUInt64('dict', 'val', toUInt64(0));
SELECT query_count FROM system.dictionaries WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'dict';


SELECT 'SYSTEM RELOAD DICTIONARIES ON CLUSTER test_shard_localhost';
SET distributed_ddl_output_mode='throw';
SYSTEM RELOAD DICTIONARIES ON CLUSTER; -- { clientError SYNTAX_ERROR }
SYSTEM RELOAD DICTIONARIES ON CLUSTER test_shard_localhost;
SET distributed_ddl_output_mode='none';
SELECT query_count FROM system.dictionaries WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'dict';
SELECT dictGetUInt64('dict', 'val', toUInt64(1));
SELECT query_count FROM system.dictionaries WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'dict';

SELECT 'CREATE DATABASE';
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
SELECT query_count FROM system.dictionaries WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'dict';

DROP DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.dict;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dict_data;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
