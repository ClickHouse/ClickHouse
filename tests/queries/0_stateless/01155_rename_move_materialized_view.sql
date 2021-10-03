-- Tags: no-parallel

DROP DATABASE IF EXISTS test_01155_ordinary;
DROP DATABASE IF EXISTS test_01155_atomic;

CREATE DATABASE test_01155_ordinary ENGINE=Ordinary;
CREATE DATABASE test_01155_atomic ENGINE=Atomic;

USE test_01155_ordinary;
CREATE TABLE src (s String) ENGINE=MergeTree() PARTITION BY tuple() ORDER BY s;
CREATE MATERIALIZED VIEW mv1 (s String) ENGINE=MergeTree() PARTITION BY tuple() ORDER BY s AS SELECT (*,).1 || 'mv1' as s FROM src;
CREATE TABLE dst (s String) ENGINE=MergeTree() PARTITION BY tuple() ORDER BY s;
CREATE MATERIALIZED VIEW mv2 TO dst (s String) AS SELECT (*,).1 || 'mv2' as s FROM src;
CREATE TABLE dist (s String) Engine=Distributed(test_shard_localhost, test_01155_ordinary, src);
INSERT INTO dist VALUES ('before moving tables');
SYSTEM FLUSH DISTRIBUTED  dist;
-- FIXME Cannot convert column `1` because it is non constant in source stream but must be constant in result
SELECT materialize(1), substr(_table, 1, 10), s FROM merge('test_01155_ordinary', '') ORDER BY _table, s;

-- Move tables with materialized views from Ordinary to Atomic
SELECT 'ordinary:';
SHOW TABLES FROM test_01155_ordinary;
RENAME TABLE test_01155_ordinary.mv1 TO test_01155_atomic.mv1;
RENAME TABLE test_01155_ordinary.mv2 TO test_01155_atomic.mv2;
RENAME TABLE test_01155_ordinary.dst TO test_01155_atomic.dst;
RENAME TABLE test_01155_ordinary.src TO test_01155_atomic.src;
RENAME TABLE test_01155_ordinary.dist TO test_01155_atomic.dist;
SELECT 'ordinary after rename:';
SELECT substr(name, 1, 10) FROM system.tables WHERE database='test_01155_ordinary';
SELECT 'atomic after rename:';
SELECT substr(name, 1, 10) FROM system.tables WHERE database='test_01155_atomic';
DROP DATABASE test_01155_ordinary;
USE default;

INSERT INTO test_01155_atomic.src VALUES ('after moving tables');
SELECT materialize(2), substr(_table, 1, 10), s FROM merge('test_01155_atomic', '') ORDER BY _table, s; -- { serverError 81 }

RENAME DATABASE test_01155_atomic TO test_01155_ordinary;
USE test_01155_ordinary;

INSERT INTO dist VALUES ('after renaming database');
SYSTEM FLUSH DISTRIBUTED  dist;
SELECT materialize(3), substr(_table, 1, 10), s FROM merge('test_01155_ordinary', '') ORDER BY _table, s;

SELECT substr(name, 1, 10) FROM system.tables WHERE database='test_01155_ordinary';

-- Move tables back
RENAME DATABASE test_01155_ordinary TO test_01155_atomic;

CREATE DATABASE test_01155_ordinary ENGINE=Ordinary;
SHOW CREATE DATABASE test_01155_atomic;

RENAME TABLE test_01155_atomic.mv1 TO test_01155_ordinary.mv1;
RENAME TABLE test_01155_atomic.mv2 TO test_01155_ordinary.mv2;
RENAME TABLE test_01155_atomic.dst TO test_01155_ordinary.dst;
RENAME TABLE test_01155_atomic.src TO test_01155_ordinary.src;
RENAME TABLE test_01155_atomic.dist TO test_01155_ordinary.dist;

INSERT INTO dist VALUES ('after renaming tables');
SYSTEM FLUSH DISTRIBUTED  dist;
SELECT materialize(4), substr(_table, 1, 10), s FROM merge('test_01155_ordinary', '') ORDER BY _table, s;
SELECT 'test_01155_ordinary:';
SHOW TABLES FROM test_01155_ordinary;
SELECT 'test_01155_atomic:';
SHOW TABLES FROM test_01155_atomic;

DROP DATABASE IF EXISTS test_01155_atomic;
DROP DATABASE IF EXISTS test_01155_ordinary;
