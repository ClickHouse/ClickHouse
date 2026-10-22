-- A later plain `DROP COLUMN` must discard the staged default an earlier subcommand
-- installed for that same column. Otherwise `validateColumnsDefaultsAndGetSampleBlock`
-- still analyzes the leftover `y AS x_tmp_alter...` pair after `x` has left the schema.

DROP TABLE IF EXISTS drop_staged_default;
CREATE TABLE drop_staged_default (k UInt8, x UInt8, y UInt8) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE drop_staged_default
    MODIFY COLUMN x DEFAULT y,
    DROP COLUMN x,
    DROP COLUMN y;
SELECT 'drop after staged default', name FROM system.columns
    WHERE database = currentDatabase() AND table = 'drop_staged_default' ORDER BY name;
DROP TABLE drop_staged_default;

-- The same through `ADD COLUMN`.
DROP TABLE IF EXISTS drop_staged_default_add;
CREATE TABLE drop_staged_default_add (k UInt8, y UInt8) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE drop_staged_default_add
    ADD COLUMN x UInt8 DEFAULT y,
    DROP COLUMN x,
    DROP COLUMN y;
SELECT 'drop after staged add default', name FROM system.columns
    WHERE database = currentDatabase() AND table = 'drop_staged_default_add' ORDER BY name;
DROP TABLE drop_staged_default_add;

-- `DROP COLUMN n` un-exists every `n.*` member, including a staged default on `n.x`.
DROP TABLE IF EXISTS drop_staged_default_nested;
CREATE TABLE drop_staged_default_nested (k UInt8, n Nested(x UInt8), y UInt8) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE drop_staged_default_nested
    MODIFY COLUMN `n.x` DEFAULT y,
    DROP COLUMN n,
    DROP COLUMN y;
SELECT 'drop nested parent after staged default', name FROM system.columns
    WHERE database = currentDatabase() AND table = 'drop_staged_default_nested' ORDER BY name;
DROP TABLE drop_staged_default_nested;

-- Dropping a dependency of a default that is still in the final schema is still rejected.
DROP TABLE IF EXISTS drop_default_dependency;
CREATE TABLE drop_default_dependency (k UInt8, x UInt8, y UInt8) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE drop_default_dependency
    MODIFY COLUMN x DEFAULT y,
    DROP COLUMN y; -- { serverError ILLEGAL_COLUMN }
DROP TABLE drop_default_dependency;
