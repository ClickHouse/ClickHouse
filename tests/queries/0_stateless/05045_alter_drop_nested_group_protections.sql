-- Regression for https://github.com/ClickHouse/ClickHouse/issues/114481

DROP TABLE IF EXISTS nested_drop_if_exists;

CREATE TABLE nested_drop_if_exists
(
    `n.a` UInt64,
    `n.b` UInt64,
    x UInt64
)
ENGINE = MergeTree
ORDER BY x;

INSERT INTO nested_drop_if_exists VALUES (1, 10, 100);
ALTER TABLE nested_drop_if_exists DROP COLUMN IF EXISTS n;

SELECT name
FROM system.columns
WHERE database = currentDatabase() AND table = 'nested_drop_if_exists'
ORDER BY position;

SELECT * FROM nested_drop_if_exists;
DROP TABLE nested_drop_if_exists;

DROP TABLE IF EXISTS nested_drop_no_shared_offsets;

CREATE TABLE nested_drop_no_shared_offsets
(
    `n.a` UInt64,
    `n.b` UInt64,
    x UInt64
)
ENGINE = MergeTree
ORDER BY x
SETTINGS share_nested_offsets = 0;

INSERT INTO nested_drop_no_shared_offsets VALUES (1, 10, 100);
ALTER TABLE nested_drop_no_shared_offsets DROP COLUMN IF EXISTS n;

SELECT name
FROM system.columns
WHERE database = currentDatabase() AND table = 'nested_drop_no_shared_offsets'
ORDER BY position;

SELECT * FROM nested_drop_no_shared_offsets;
DROP TABLE nested_drop_no_shared_offsets;

DROP TABLE IF EXISTS nested_drop_default_dependency;

CREATE TABLE nested_drop_default_dependency
(
    `n.a` UInt64,
    x UInt64 DEFAULT `n.a`
)
ENGINE = MergeTree
ORDER BY tuple();

SET allow_experimental_analyzer = 0;

ALTER TABLE nested_drop_default_dependency DROP COLUMN n; -- { serverError ILLEGAL_COLUMN }
ALTER TABLE nested_drop_default_dependency DROP COLUMN IF EXISTS n; -- { serverError ILLEGAL_COLUMN }

SET allow_experimental_analyzer = 1;

ALTER TABLE nested_drop_default_dependency DROP COLUMN n; -- { serverError ILLEGAL_COLUMN }
ALTER TABLE nested_drop_default_dependency DROP COLUMN IF EXISTS n; -- { serverError ILLEGAL_COLUMN }
DROP TABLE nested_drop_default_dependency;

DROP VIEW IF EXISTS nested_drop_mv;
DROP TABLE IF EXISTS nested_drop_mv_source;

CREATE TABLE nested_drop_mv_source
(
    `n.a` UInt64,
    `n.b` UInt64,
    x UInt64
)
ENGINE = MergeTree
ORDER BY x;

CREATE MATERIALIZED VIEW nested_drop_mv
ENGINE = Null
AS SELECT `n.a` FROM nested_drop_mv_source;

ALTER TABLE nested_drop_mv_source DROP COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE nested_drop_mv_source DROP COLUMN IF EXISTS n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP VIEW nested_drop_mv;
DROP TABLE nested_drop_mv_source;

DROP TABLE IF EXISTS nested_drop_unfinished_mutation;

CREATE TABLE nested_drop_unfinished_mutation
(
    `n.a` UInt64,
    x UInt64,
    c UInt64
)
ENGINE = MergeTree
ORDER BY x;

INSERT INTO nested_drop_unfinished_mutation VALUES (1, 1, 1);
SYSTEM STOP MERGES nested_drop_unfinished_mutation;
ALTER TABLE nested_drop_unfinished_mutation UPDATE c = `n.a` + 1 WHERE 1 SETTINGS mutations_sync = 0;

ALTER TABLE nested_drop_unfinished_mutation DROP COLUMN n; -- { serverError BAD_ARGUMENTS }
ALTER TABLE nested_drop_unfinished_mutation DROP COLUMN IF EXISTS n; -- { serverError BAD_ARGUMENTS }

KILL MUTATION
WHERE database = currentDatabase() AND table = 'nested_drop_unfinished_mutation'
SYNC
FORMAT Null;

SYSTEM START MERGES nested_drop_unfinished_mutation;
DROP TABLE nested_drop_unfinished_mutation;