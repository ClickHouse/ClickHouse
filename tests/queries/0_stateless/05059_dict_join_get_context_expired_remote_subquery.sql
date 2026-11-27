-- A function that reads the query context at execution time (`dictGet`, `joinGet`) must keep
-- that context alive: the analyzer shares one resolved function instance between scopes whose
-- query-tree hash is equal, so the planning scope of a distributed sub-query can hand its
-- instance to the outer projection.
-- https://github.com/ClickHouse/ClickHouse/issues/117753

SET enable_analyzer = 1;

CREATE TABLE dict_src (id UInt64, attr String) ENGINE = MergeTree ORDER BY id AS SELECT 1, '1';
CREATE DICTIONARY d (id UInt64, attr String) PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'dict_src')) LIFETIME(0) LAYOUT(HASHED());

CREATE TABLE j (id UInt64, attr String) ENGINE = Join(ANY, LEFT, id);
INSERT INTO j VALUES (1, '1');

CREATE TABLE c (k UInt64) ENGINE = MergeTree ORDER BY k AS SELECT 1;
CREATE TABLE f (id UInt64, c UInt64) ENGINE = MergeTree ORDER BY id AS SELECT 1, 1;

-- The dictionary and `Join` names are qualified because both are resolved against the current
-- database of whichever node executes the function, which is not the initiator when
-- `prefer_localhost_replica` is 0.

-- The same lambda argument name in both scopes.
SELECT arrayMap(xx -> dictGetString(currentDatabase() || '.d', 'attr', xx), [id])
FROM (
    SELECT id FROM remote('127.0.0.1', currentDatabase(), f)
    WHERE c IN (SELECT k FROM c WHERE arrayMap(xx -> dictGetString(currentDatabase() || '.d', 'attr', xx), [k]) = ['1'])
);

-- The same sub-expression in both scopes, without a lambda.
SELECT dictGetString(currentDatabase() || '.d', 'attr', materialize(toUInt64(1)))
FROM (
    SELECT id FROM remote('127.0.0.1', currentDatabase(), f)
    WHERE c IN (SELECT k FROM c WHERE dictGetString(currentDatabase() || '.d', 'attr', materialize(toUInt64(1))) = '1')
);

SELECT arrayMap(xx -> joinGet(currentDatabase() || '.j', 'attr', xx), [id])
FROM (
    SELECT id FROM remote('127.0.0.1', currentDatabase(), f)
    WHERE c IN (SELECT k FROM c WHERE arrayMap(xx -> joinGet(currentDatabase() || '.j', 'attr', xx), [k]) = ['1'])
);

SELECT joinGet(currentDatabase() || '.j', 'attr', materialize(toUInt64(1)))
FROM (
    SELECT id FROM remote('127.0.0.1', currentDatabase(), f)
    WHERE c IN (SELECT k FROM c WHERE joinGet(currentDatabase() || '.j', 'attr', materialize(toUInt64(1))) = '1')
);
