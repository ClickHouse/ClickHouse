-- Tags: zookeeper, no-ordinary-database, no-fasttest

SET ast_fuzzer_runs = 0;
SET ast_fuzzer_any_query = 0;
SET ignore_drop_queries_probability = 0;

DROP TABLE IF EXISTS 05043_keeper_map_canonical_writer_parenthesized SYNC;
DROP TABLE IF EXISTS 05043_keeper_map_canonical_writer_nested_second SYNC;
DROP TABLE IF EXISTS 05043_keeper_map_canonical_writer_nested SYNC;
DROP TABLE IF EXISTS 05043_keeper_map_canonical_writer_required SYNC;
DROP TABLE IF EXISTS 05043_keeper_map_canonical_writer_expression_list SYNC;

CREATE TABLE 05043_keeper_map_canonical_writer_parenthesized (key UInt64, value String)
ENGINE = KeeperMap('/' || currentDatabase() || '/05043_keeper_map_canonical_writer_parenthesized')
PRIMARY KEY(key);

SELECT name, endsWith(value, 'primary key: key\n')
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05043_keeper_map_canonical_writer_parenthesized'
    AND name IN ('data', 'metadata')
ORDER BY name;

CREATE TABLE 05043_keeper_map_canonical_writer_nested (key UInt64, value String)
ENGINE = KeeperMap('/' || currentDatabase() || '/05043_keeper_map_canonical_writer_nested')
PRIMARY KEY((key) + 1);

SELECT name, endsWith(value, 'primary key: (key) + 1\n')
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05043_keeper_map_canonical_writer_nested'
    AND name IN ('data', 'metadata')
ORDER BY name;

-- Simulate metadata written before canonical writes were introduced. A reader-first rollout must
-- accept it and must not rewrite either shared znode while another table attaches to the path.
INSERT INTO system.zookeeper (path, name, value)
SELECT path, name, replaceOne(value, 'primary key: (key) + 1\n', 'primary key: ((key) + 1)\n')
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05043_keeper_map_canonical_writer_nested'
    AND name IN ('data', 'metadata');

CREATE TABLE 05043_keeper_map_canonical_writer_nested_second (key UInt64, value String)
ENGINE = KeeperMap('/' || currentDatabase() || '/05043_keeper_map_canonical_writer_nested')
PRIMARY KEY((key) + 1);

SELECT count() = 2 AND countIf(endsWith(value, 'primary key: ((key) + 1)\n')) = 2
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05043_keeper_map_canonical_writer_nested'
    AND name IN ('data', 'metadata');

DETACH TABLE 05043_keeper_map_canonical_writer_nested;
ATTACH TABLE 05043_keeper_map_canonical_writer_nested;

INSERT INTO 05043_keeper_map_canonical_writer_nested VALUES (1, 'value');
SELECT * FROM 05043_keeper_map_canonical_writer_nested_second;

CREATE TABLE 05043_keeper_map_canonical_writer_required (key UInt64, value String)
ENGINE = KeeperMap('/' || currentDatabase() || '/05043_keeper_map_canonical_writer_required')
PRIMARY KEY(((key + 1) * 2));

SELECT name, endsWith(value, 'primary key: (key + 1) * 2\n')
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05043_keeper_map_canonical_writer_required'
    AND name IN ('data', 'metadata')
ORDER BY name;

CREATE TABLE 05043_keeper_map_canonical_writer_expression_list (key UInt64, value String)
ENGINE = KeeperMap('/' || currentDatabase() || '/05043_keeper_map_canonical_writer_expression_list')
PRIMARY KEY((key), sipHash64(key));

-- The compatibility reader from #115642 canonicalizes one complete expression. Preserve the
-- existing spelling for expression lists until every supported reader understands that format.
SELECT name, endsWith(value, 'primary key: (key), sipHash64(key)\n')
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05043_keeper_map_canonical_writer_expression_list'
    AND name IN ('data', 'metadata')
ORDER BY name;

DROP TABLE 05043_keeper_map_canonical_writer_parenthesized SYNC;
DROP TABLE 05043_keeper_map_canonical_writer_nested_second SYNC;
DROP TABLE 05043_keeper_map_canonical_writer_nested SYNC;
DROP TABLE 05043_keeper_map_canonical_writer_required SYNC;
DROP TABLE 05043_keeper_map_canonical_writer_expression_list SYNC;
