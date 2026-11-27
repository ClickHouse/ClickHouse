-- Regression test for the plain `CREATE TABLE ... AS SELECT` temporary-table publish path (issue #26746):
-- the up-front existence fast path in `doCreateOrReplaceTable` must mirror the full existence handling of
-- `doCreateTable`, not only the active-table `isTableExist` probe:
--   * a name already used by a dictionary reports `DICTIONARY_ALREADY_EXISTS`, not `TABLE_ALREADY_EXISTS`;
--   * a name reserved by a detached / detached-permanently table is handled before the populate, so
--     `IF NOT EXISTS` is a no-op (the SELECT is not run) and a plain create fails with the detached-name
--     error -- not with a source-query failure that would only surface once the populate runs.

DROP TABLE IF EXISTS src;
DROP DICTIONARY IF EXISTS dict_x;
DROP TABLE IF EXISTS t_detached;

CREATE TABLE src (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (1, 10);

-- A dictionary occupies the name `dict_x`.
CREATE DICTIONARY dict_x (id UInt64, v UInt64) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src'))
LAYOUT(FLAT())
LIFETIME(0);

-- `CREATE TABLE ... AS SELECT` over an existing dictionary name must report `DICTIONARY_ALREADY_EXISTS`
-- (the same as a plain `CREATE TABLE` would, see `02973_dictionary_table_exception_fix`), not
-- `TABLE_ALREADY_EXISTS`.
SELECT 'dictionary_collision';
CREATE TABLE dict_x ENGINE = MergeTree ORDER BY id AS SELECT number AS id, number AS v FROM numbers(3); -- { serverError DICTIONARY_ALREADY_EXISTS }
-- `IF NOT EXISTS` over the dictionary name is a no-op: `throwIf(1)` in the SELECT must never be evaluated.
CREATE TABLE IF NOT EXISTS dict_x ENGINE = MergeTree ORDER BY id AS SELECT throwIf(1) AS id, throwIf(1) AS v;
SELECT 'dictionary_untouched', count() FROM system.tables WHERE database = currentDatabase() AND name = 'dict_x';

-- A detached-permanently table reserves the name `t_detached`: `isTableExist` is false but the metadata
-- file is present.
CREATE TABLE t_detached (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_detached VALUES (7);
DETACH TABLE t_detached PERMANENTLY;

-- `IF NOT EXISTS` over the detached name is a no-op: `throwIf(1)` in the SELECT must never be evaluated.
SELECT 'detached_if_not_exists_noop';
CREATE TABLE IF NOT EXISTS t_detached ENGINE = MergeTree ORDER BY x AS SELECT throwIf(1) AS x;

-- A plain create over the detached name fails fast with the detached-name error (`TABLE_ALREADY_EXISTS`
-- from the metadata check) before running the SELECT, so `throwIf(1)` must not fire either.
SELECT 'detached_plain_create';
CREATE TABLE t_detached ENGINE = MergeTree ORDER BY x AS SELECT throwIf(1) AS x; -- { serverError TABLE_ALREADY_EXISTS }

-- The detached table is intact and can still be attached with its original data.
ATTACH TABLE t_detached;
SELECT 'detached_intact', count(), sum(x) FROM t_detached;

DROP TABLE t_detached;
DROP DICTIONARY dict_x;
DROP TABLE src;
