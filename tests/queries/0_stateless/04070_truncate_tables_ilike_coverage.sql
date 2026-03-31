-- Tags: no-fasttest, no-replicated-database

DROP TABLE IF EXISTS ilike_trunc_FOO;
DROP TABLE IF EXISTS ilike_trunc_foo;
DROP TABLE IF EXISTS ilike_trunc_Bar;
DROP TABLE IF EXISTS ilike_trunc_skip;

CREATE TABLE ilike_trunc_FOO (n UInt64) ENGINE = MergeTree() ORDER BY n;
CREATE TABLE ilike_trunc_foo (n UInt64) ENGINE = MergeTree() ORDER BY n;
CREATE TABLE ilike_trunc_Bar (n UInt64) ENGINE = MergeTree() ORDER BY n;
CREATE TABLE ilike_trunc_skip (n UInt64) ENGINE = MergeTree() ORDER BY n;

INSERT INTO ilike_trunc_FOO VALUES (1);
INSERT INTO ilike_trunc_foo VALUES (2);
INSERT INTO ilike_trunc_Bar VALUES (3);
INSERT INTO ilike_trunc_skip VALUES (4);

-- ILIKE: should match ilike_trunc_FOO and ilike_trunc_foo (case-insensitive) but not Bar or skip
TRUNCATE TABLES FROM {CLICKHOUSE_DATABASE:Identifier} ILIKE '%\\_foo';

SELECT 'FOO', count() FROM ilike_trunc_FOO;
SELECT 'foo', count() FROM ilike_trunc_foo;
SELECT 'Bar', count() FROM ilike_trunc_Bar;
SELECT 'skip', count() FROM ilike_trunc_skip;

-- Repopulate
INSERT INTO ilike_trunc_FOO VALUES (1);
INSERT INTO ilike_trunc_foo VALUES (2);

-- NOT ILIKE: truncate everything NOT matching '%_foo' case-insensitively
TRUNCATE TABLES FROM {CLICKHOUSE_DATABASE:Identifier} NOT ILIKE '%\\_foo';

SELECT 'FOO_after_not', count() FROM ilike_trunc_FOO;
SELECT 'foo_after_not', count() FROM ilike_trunc_foo;
SELECT 'Bar_after_not', count() FROM ilike_trunc_Bar;
SELECT 'skip_after_not', count() FROM ilike_trunc_skip;

DROP TABLE IF EXISTS ilike_trunc_FOO;
DROP TABLE IF EXISTS ilike_trunc_foo;
DROP TABLE IF EXISTS ilike_trunc_Bar;
DROP TABLE IF EXISTS ilike_trunc_skip;
