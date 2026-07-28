-- A read-only `Overlay` facade forwards the `system.tables` name filter (the `TablesFilter` hint)
-- to each of its source databases, so a source able to push the hint down to an external catalog
-- keeps doing so behind the facade, and the names-only listing stays a names-only listing instead
-- of falling back to the heavyweight iterator that resolves the storage of every source table.
-- This test pins the observable contract of those overrides: whatever the filter shape, the facade
-- lists exactly the union of its sources' tables, and a shadowed name appears once.

DROP DATABASE IF EXISTS db_ovl_04650;
DROP DATABASE IF EXISTS db_src_a_04650;
DROP DATABASE IF EXISTS db_src_b_04650;

CREATE DATABASE db_src_a_04650;
CREATE DATABASE db_src_b_04650;

CREATE TABLE db_src_a_04650.pref_one (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE db_src_a_04650.pref_two (x UInt64) ENGINE = Memory;
CREATE TABLE db_src_a_04650.other (x UInt64) ENGINE = Memory;
-- The same name in both sources: the first listed source wins, and the facade reports one row.
CREATE TABLE db_src_a_04650.shadowed (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE db_src_b_04650.shadowed (y String) ENGINE = Memory;
CREATE TABLE db_src_b_04650.only_in_b (x UInt64) ENGINE = Memory;

CREATE DATABASE db_ovl_04650 ENGINE = Overlay('db_src_a_04650', 'db_src_b_04650');

SELECT 'no filter';
SELECT name FROM system.tables WHERE database = 'db_ovl_04650' ORDER BY name;

SELECT 'equality filter, names only';
SELECT name FROM system.tables WHERE database = 'db_ovl_04650' AND name = 'pref_one';
SELECT name FROM system.tables WHERE database = 'db_ovl_04650' AND name = 'only_in_b';
SELECT name FROM system.tables WHERE database = 'db_ovl_04650' AND name = 'no_such_table';

-- A filter on `engine` or `uuid` needs the source storage, so this takes the heavyweight hint
-- iterator instead of the names-only one. A table listed under the facade reports a nil `uuid`,
-- because the facade name is not the identity of the table; the filtered listing must agree with
-- the unfiltered one about that.
SELECT 'equality filter, engine and uuid resolved through the facade';
SELECT name, engine FROM system.tables WHERE database = 'db_ovl_04650' AND name = 'pref_one';
SELECT name, uuid FROM system.tables WHERE database = 'db_ovl_04650' AND name = 'pref_one';
SELECT name, uuid FROM system.tables WHERE database = 'db_ovl_04650' ORDER BY name;

SELECT 'LIKE filter';
SELECT name FROM system.tables WHERE database = 'db_ovl_04650' AND name LIKE 'pref\\_%' ORDER BY name;

SELECT 'startsWith filter';
SELECT name FROM system.tables WHERE database = 'db_ovl_04650' AND startsWith(name, 'only') ORDER BY name;

SELECT 'a shadowed name is reported once, resolved to the first source';
SELECT name, engine FROM system.tables WHERE database = 'db_ovl_04650' AND name = 'shadowed';

SELECT 'SHOW TABLES with a pattern';
SHOW TABLES FROM db_ovl_04650 LIKE 'pref\\_%';

SELECT 'the sources themselves are unaffected';
SELECT database, name FROM system.tables WHERE database = 'db_src_b_04650' ORDER BY name;

DROP DATABASE db_ovl_04650;
DROP DATABASE db_src_a_04650;
DROP DATABASE db_src_b_04650;
