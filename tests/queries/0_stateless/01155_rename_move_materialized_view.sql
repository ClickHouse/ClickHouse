-- Tags: no-parallel

SET send_logs_level = 'fatal';
SET prefer_localhost_replica = 1;

DROP DATABASE IF EXISTS test_01155_ordinary;
DROP DATABASE IF EXISTS test_01155_atomic;

set allow_deprecated_database_ordinary=1;
-- Creation of a database with Ordinary engine emits a warning.
CREATE DATABASE test_01155_ordinary ENGINE=Ordinary;
CREATE DATABASE test_01155_atomic ENGINE=Atomic;

USE test_01155_ordinary;
CREATE TABLE src (s String, x String DEFAULT 'a') ENGINE=MergeTree() PARTITION BY tuple() ORDER BY s;
CREATE MATERIALIZED VIEW mv1 (s String, x String DEFAULT 'b') ENGINE=MergeTree() PARTITION BY tuple() ORDER BY s AS SELECT (*,).1 || 'mv1' as s FROM src;
CREATE TABLE dst (s String, x String DEFAULT 'c') ENGINE=MergeTree() PARTITION BY tuple() ORDER BY s;
CREATE MATERIALIZED VIEW mv2 TO dst (s String, x String DEFAULT 'd') AS SELECT (*,).1 || 'mv2' as s FROM src;
CREATE TABLE dist (s String, x String DEFAULT 'asdf') ENGINE=Distributed(test_shard_localhost, test_01155_ordinary, src);
INSERT INTO dist(s) VALUES ('before moving tables');
SYSTEM FLUSH DISTRIBUTED  dist;

CREATE DICTIONARY dict (s String, x String DEFAULT 'qwerty') PRIMARY KEY s
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dist' DB 'test_01155_ordinary'))
LIFETIME(MIN 0 MAX 2) LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 123));

-- FIXME Cannot convert column `1` because it is non constant in source stream but must be constant in result
SELECT materialize(1), substr(_table, 1, 10), s FROM merge('test_01155_ordinary', '') ORDER BY _table, s;
SELECT dictGet('test_01155_ordinary.dict', 'x', 'before moving tables');

RENAME DICTIONARY test_01155_ordinary.dict TO test_01155_ordinary.dict1;
SELECT dictGet('test_01155_ordinary.dict1', 'x', 'before moving tables');
SELECT database, name, uuid FROM system.dictionaries WHERE database='test_01155_ordinary';
RENAME TABLE test_01155_ordinary.dict1 TO test_01155_ordinary.dict;
SELECT dictGet('test_01155_ordinary.dict', 'x', 'before moving tables');

-- Move tables with materialized views from Ordinary to Atomic
SELECT 'ordinary:';
SHOW TABLES FROM test_01155_ordinary;
RENAME TABLE test_01155_ordinary.mv1 TO test_01155_atomic.mv1;
RENAME TABLE test_01155_ordinary.mv2 TO test_01155_atomic.mv2;
RENAME TABLE test_01155_ordinary.dst TO test_01155_atomic.dst;
RENAME TABLE test_01155_ordinary.src TO test_01155_atomic.src;
SET check_table_dependencies=0; -- Otherwise we'll get error "test_01155_ordinary.dict depends on test_01155_ordinary.dist" in the next line.
RENAME TABLE test_01155_ordinary.dist TO test_01155_atomic.dist;
SET check_table_dependencies=1;
RENAME DICTIONARY test_01155_ordinary.dict TO test_01155_atomic.dict;
SELECT 'ordinary after rename:';
SELECT substr(name, 1, 10) FROM system.tables WHERE database='test_01155_ordinary';
SELECT 'atomic after rename:';
SELECT substr(name, 1, 10) FROM system.tables WHERE database='test_01155_atomic';
DROP DATABASE test_01155_ordinary;
USE default;

INSERT INTO test_01155_atomic.src(s) VALUES ('after moving tables');
SELECT materialize(2), substr(_table, 1, 10), s FROM merge('test_01155_atomic', '') ORDER BY _table, s; -- { serverError UNKNOWN_DATABASE }
SELECT dictGet('test_01155_ordinary.dict', 'x', 'after moving tables'); -- { serverError BAD_ARGUMENTS }

RENAME DATABASE test_01155_atomic TO test_01155_ordinary;
USE test_01155_ordinary;

INSERT INTO dist(s) VALUES ('after renaming database');
SYSTEM FLUSH DISTRIBUTED  dist;
SELECT materialize(3), substr(_table, 1, 10), s FROM merge('test_01155_ordinary', '') ORDER BY _table, s;
SELECT dictGet('test_01155_ordinary.dict', 'x', 'after renaming database');

SELECT database, substr(name, 1, 10) FROM system.tables WHERE database like 'test_01155_%';

-- Move tables back
SET check_table_dependencies=0; -- Otherwise we'll get error "test_01155_ordinary.dict depends on test_01155_ordinary.dist" in the next line.
RENAME DATABASE test_01155_ordinary TO test_01155_atomic;
SET check_table_dependencies=1;

set allow_deprecated_database_ordinary=1;
-- Creation of a database with Ordinary engine emits a warning.
SET send_logs_level='fatal';
CREATE DATABASE test_01155_ordinary ENGINE=Ordinary;
SET send_logs_level='warning';
SHOW CREATE DATABASE test_01155_atomic;

RENAME TABLE test_01155_atomic.mv1 TO test_01155_ordinary.mv1;
RENAME TABLE test_01155_atomic.mv2 TO test_01155_ordinary.mv2;
RENAME TABLE test_01155_atomic.dst TO test_01155_ordinary.dst;
RENAME TABLE test_01155_atomic.src TO test_01155_ordinary.src;
RENAME TABLE test_01155_atomic.dist TO test_01155_ordinary.dist;
RENAME DICTIONARY test_01155_atomic.dict TO test_01155_ordinary.dict;

INSERT INTO dist(s) VALUES ('after renaming tables');
SYSTEM FLUSH DISTRIBUTED  dist;
SELECT materialize(4), substr(_table, 1, 10), s FROM merge('test_01155_ordinary', '') ORDER BY _table, s;
SELECT dictGet('test_01155_ordinary.dict', 'x', 'after renaming tables');
SELECT database, name, uuid FROM system.dictionaries WHERE database='test_01155_ordinary';
SELECT 'test_01155_ordinary:';
SHOW TABLES FROM test_01155_ordinary;
SELECT 'test_01155_atomic:';
SHOW TABLES FROM test_01155_atomic;

DROP DATABASE IF EXISTS test_01155_atomic;
DROP DATABASE IF EXISTS test_01155_ordinary;
