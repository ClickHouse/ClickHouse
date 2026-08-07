-- Tags: no-ordinary-database, no-fasttest

-- `KeeperMap` stores the primary key as text in Keeper and compares it there against the text
-- written by whichever server version created the table. Since
-- https://github.com/ClickHouse/ClickHouse/pull/92340 the formatter preserves the parentheses the
-- user wrote, so `PRIMARY KEY(key)` was serialized as `(key)` while every earlier version wrote
-- `key`, and a server of the other version refused to open the table with
-- `Path ... is already used but the stored primary key definition doesn't match`.
-- The stored form must not depend on how the primary key was spelled.

DROP TABLE IF EXISTS t_km_parens SYNC;
DROP TABLE IF EXISTS t_km_no_parens SYNC;

CREATE TABLE t_km_parens (key UInt64, value String)
Engine=KeeperMap('/' || currentDatabase() || '/km_parens') PRIMARY KEY(key);

SELECT extract(value, 'primary key: (.*)') FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/km_parens' AND name = 'metadata';

-- The same path, with the primary key spelled without parentheses: the metadata must be
-- recognized as the same, which is what a server of a different version does.
CREATE TABLE t_km_no_parens (key UInt64, value String)
Engine=KeeperMap('/' || currentDatabase() || '/km_parens') PRIMARY KEY key;

INSERT INTO t_km_parens VALUES (1, 'a');
SELECT * FROM t_km_no_parens ORDER BY ALL;

-- A genuinely different primary key must still be rejected.
CREATE TABLE t_km_bad (key UInt64, value String)
Engine=KeeperMap('/' || currentDatabase() || '/km_parens') PRIMARY KEY(value); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_km_parens SYNC;
DROP TABLE t_km_no_parens SYNC;
