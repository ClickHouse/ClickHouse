-- Tags: no-old-analyzer
-- The old analyzer qualifies the dictionary name only in the query it sends to the shards, while the
-- header it computes on the initiator keeps the unqualified column name, so these queries fail there
-- with NOT_FOUND_COLUMN_IN_BLOCK. Only the analyzer resolves the name consistently on both sides.

DROP DICTIONARY IF EXISTS dict_current_database;
DROP TABLE IF EXISTS dict_source;
DROP TABLE IF EXISTS local_table;
DROP TABLE IF EXISTS distributed_table;

CREATE TABLE dict_source (code UInt64, value String) ENGINE = MergeTree ORDER BY code;
INSERT INTO dict_source VALUES (1, 'one'), (2, 'two');

CREATE DICTIONARY dict_current_database (code UInt64, value String)
PRIMARY KEY code
SOURCE(CLICKHOUSE(HOST '127.0.0.1' PORT tcpPort() DB currentDatabase() TABLE 'dict_source'))
LAYOUT(HASHED())
LIFETIME(0);

CREATE TABLE local_table (code UInt64) ENGINE = MergeTree ORDER BY code;
INSERT INTO local_table VALUES (1), (2);

CREATE TABLE distributed_table (code UInt64)
ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), local_table, rand());

-- Both shards of `test_cluster_two_shards` are addresses of this very server, so the initiator would
-- run the shard queries locally - in its own session, where the dictionary resolves anyway. Force
-- real connections, whose current database is `default` and not the database of this test.
SET prefer_localhost_replica = 0;

SELECT code, dictGet('dict_current_database', 'value', code) FROM distributed_table ORDER BY ALL;
SELECT code, dictGet(dict_current_database, 'value', code) FROM distributed_table ORDER BY ALL;
SELECT code, dictGetOrDefault('dict_current_database', 'value', code, 'none') FROM distributed_table ORDER BY ALL;
SELECT code, dictHas('dict_current_database', code) FROM distributed_table ORDER BY ALL;

-- The dictionary name has to be bound in every part of the query, not only in the projection.
SELECT count() FROM distributed_table WHERE dictGet('dict_current_database', 'value', code) = 'one';
SELECT count() FROM distributed_table GROUP BY dictGet('dict_current_database', 'value', code) ORDER BY ALL;
SELECT code FROM distributed_table ORDER BY dictGet('dict_current_database', 'value', code) DESC, code;
SELECT code FROM distributed_table WHERE code IN (SELECT code FROM local_table WHERE dictGet('dict_current_database', 'value', code) = 'two') ORDER BY ALL;

SELECT code, dictGet('dict_current_database', 'value', code)
FROM cluster(test_cluster_two_shards, currentDatabase(), local_table) ORDER BY ALL;

-- Binding the name must not change the name of the column: it stays exactly as it was written,
-- both for a distributed and for a local query.
SELECT dictGet('dict_current_database', 'value', code) FROM distributed_table ORDER BY ALL LIMIT 1 FORMAT TSVWithNames;
SELECT dictGet('dict_current_database', 'value', code) FROM local_table ORDER BY ALL LIMIT 1 FORMAT TSVWithNames;

DROP TABLE distributed_table;
DROP TABLE local_table;
DROP DICTIONARY dict_current_database;
DROP TABLE dict_source;
