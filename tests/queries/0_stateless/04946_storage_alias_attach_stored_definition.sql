-- Tags: need-query-parameters

-- An Alias whose target does not exist yet is accepted, so the target can later become an Alias
-- itself. Loading such stored metadata must succeed: on that path a rejection fails the whole
-- metadata load, not the one table.

DROP TABLE IF EXISTS base_table;
DROP TABLE IF EXISTS outer_alias;
DROP TABLE IF EXISTS inner_alias;

CREATE TABLE base_table (a Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO base_table VALUES (1), (2);

-- The target is absent, so nothing rejects this yet.
CREATE TABLE outer_alias ENGINE = Alias(currentDatabase(), inner_alias);
CREATE TABLE inner_alias ENGINE = Alias(currentDatabase(), base_table);

SELECT '-- a stored Alias-to-Alias definition loads';
DETACH TABLE outer_alias;
ATTACH TABLE outer_alias;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'outer_alias';

SELECT '-- CREATE still rejects an Alias target';
CREATE TABLE rejected_at_create ENGINE = Alias(currentDatabase(), inner_alias); -- { serverError BAD_ARGUMENTS }

SELECT '-- a full-definition ATTACH still rejects an Alias target';
ATTACH TABLE rejected_at_full_attach UUID '9c3d1f22-0000-4000-8000-04946a000001' ENGINE = Alias(currentDatabase(), inner_alias); -- { serverError BAD_ARGUMENTS }

SELECT '-- CREATE still rejects a self-reference';
CREATE TABLE self_ref ENGINE = Alias(currentDatabase(), self_ref); -- { serverError BAD_ARGUMENTS }

SELECT '-- a full-definition ATTACH still rejects a self-reference';
ATTACH TABLE self_ref_attach UUID '9c3d1f22-0000-4000-8000-04946a000002' ENGINE = Alias(currentDatabase(), self_ref_attach); -- { serverError BAD_ARGUMENTS }

SELECT '-- reading through the chain reaches the target';
SELECT * FROM outer_alias ORDER BY a;

DROP TABLE outer_alias;
DROP TABLE inner_alias;
DROP TABLE base_table;

-- RENAME DATABASE moves tables without rewriting stored ENGINE arguments and has no referential
-- preflight, so it is a second way to store a definition the constructor once refused.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- The renamed-to database is absent, so this names a table that does not exist yet.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.renamed ENGINE = Alias({CLICKHOUSE_DATABASE_2:Identifier}, renamed);
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier};

SELECT '-- a stored self-referential definition loads';
DETACH TABLE {CLICKHOUSE_DATABASE_2:Identifier}.renamed;
ATTACH TABLE {CLICKHOUSE_DATABASE_2:Identifier}.renamed;

SELECT '-- and reading it is bounded';
SELECT * FROM {CLICKHOUSE_DATABASE_2:Identifier}.renamed; -- { serverError TOO_DEEP_RECURSION }

DROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.renamed;
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
