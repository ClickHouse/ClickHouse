-- The statement is a thin rewrite over `system.table_settings`. These check that it parses, that it
-- is told apart from the two statements it shares a prefix with, and that its filters reach through.

DROP TABLE IF EXISTS mt;
-- `Memory` rather than `MergeTree`, deliberately: it has five settings and no server configuration
-- section sets any of them. A `<merge_tree>` section does exist on real servers - the CI ones set a
-- dozen settings - and an unfiltered `SHOW CHANGED` on a `MergeTree` table then lists whatever that
-- section happened to change, which is not the same on two installations. What is under test here
-- is the statement and its filters, and those need a table whose settings only this test writes.
CREATE TABLE mt (a UInt64) ENGINE = Memory SETTINGS max_bytes_to_keep = 8192, min_bytes_to_keep = 4096;

SELECT '-- every setting, changed or not';
SHOW TABLE SETTINGS FROM mt;

SELECT '-- only what something other than the default set';
SHOW CHANGED TABLE SETTINGS FROM mt;

SELECT '-- CHANGED is about who set it, not whether the value differs';
-- `max_rows_to_keep = 0` states the default. It is still reported as changed, because the
-- definition acted on it - which is what `source != default` means and what the statement
-- documentation now says.
CREATE TABLE stated_default (a UInt64) ENGINE = Memory SETTINGS max_rows_to_keep = 0;
SHOW CHANGED TABLE SETTINGS FROM stated_default;
DROP TABLE stated_default;

SELECT '-- one setting, by name';
SHOW TABLE SETTINGS FROM mt LIKE 'min_bytes_to_keep';

SELECT '-- ILIKE is case-insensitive';
SHOW CHANGED TABLE SETTINGS FROM mt ILIKE 'MIN_BYTES%';

SELECT '-- NOT inverts it';
SHOW CHANGED TABLE SETTINGS FROM mt NOT LIKE 'min_bytes%';

SELECT '-- IN is accepted in place of FROM';
SHOW CHANGED TABLE SETTINGS IN mt LIKE 'min_bytes%';

SELECT '-- a setting is findable by a name it answers to, not only the one it is declared under';
-- `system.table_settings` carries a row per alias so a lookup by the name you know finds the
-- setting. The statement has to match the pattern against those names too, or it hands whoever
-- knows only the old spelling the empty result the alias rows exist to prevent. The row printed is
-- the canonical one either way, and asking by the declared name must not print it twice.
DROP TABLE IF EXISTS aliased;
CREATE TABLE aliased (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS enable_block_number_column = 1;
SHOW TABLE SETTINGS FROM aliased LIKE 'allow_experimental_block_number_column';
SHOW TABLE SETTINGS FROM aliased LIKE 'enable_block_number_column';
SHOW TABLE SETTINGS FROM aliased ILIKE 'ALLOW_EXPERIMENTAL_BLOCK%';

-- `NOT` drops the setting whichever of its names the pattern gives, which keeps the two forms
-- symmetric. Not asserted here: the alias lives in `MergeTree`, an unfiltered listing of a
-- `MergeTree` table's changed settings includes whatever a server's `<merge_tree>` section sets,
-- and `NOT` cannot be narrowed by a second pattern the way the positive forms can. Asserting it
-- would pin this test to one server's configuration, which is how it broke twice before.
DROP TABLE aliased;

SELECT '-- a database-qualified name parses and resolves';
-- `system.one` has no settings, so this returns nothing; what it checks is that the qualified form
-- reaches the right table rather than being read as a bare name.
SHOW TABLE SETTINGS FROM system.one;

SELECT '-- the statements sharing its prefix still parse as themselves';
SHOW TABLES;
SHOW SETTINGS LIKE 'add_http_cors_header';

SELECT '-- the AST survives a JSON round trip, as it does for SHOW COLUMNS';
SELECT formatQueryFromJSON(parseQueryToJSON($$SHOW TABLE SETTINGS FROM tbl$$));
SELECT formatQueryFromJSON(parseQueryToJSON($$SHOW CHANGED TABLE SETTINGS FROM db.tbl LIKE 'a%'$$));
SELECT formatQueryFromJSON(parseQueryToJSON($$SHOW TABLE SETTINGS FROM tbl NOT ILIKE 'x%'$$));

SELECT '-- and a payload the parser could never have produced is rejected';
SELECT formatQueryFromJSON('{"type":"ShowTableSettingsQuery","table":""}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ShowTableSettingsQuery","table":"t","not_like":true}'); -- { serverError BAD_ARGUMENTS }

DROP TABLE mt;
