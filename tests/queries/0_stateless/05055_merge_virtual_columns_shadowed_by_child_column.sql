-- The `_database`/`_table` virtual columns of a `Merge` table always carry the name of the matched
-- child, even when a child has a physical column of the same name. Such a column is never
-- requested by the `Merge` table, but it can still reach the child's stream through the substitute
-- for a read of nothing but virtual columns, through a column a row policy filter needs, or through
-- one of the child's `ALIAS` columns - and it used to win over the name the child-pruning filter in
-- `getSelectedTables` matches, so `WHERE _table = '<child>'` silently returned no rows.

DROP TABLE IF EXISTS 05045_child;
DROP TABLE IF EXISTS 05045_m;
DROP TABLE IF EXISTS 05045_alias_child;
DROP TABLE IF EXISTS 05045_alias_m;
DROP TABLE IF EXISTS 05045_only_child;
DROP TABLE IF EXISTS 05045_own_m;
DROP TABLE IF EXISTS 05045_only_m;
DROP TABLE IF EXISTS 05045_inner;
DROP TABLE IF EXISTS 05045_outer;

CREATE TABLE 05045_child (`_database` UInt8, `_table` UInt8, x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO 05045_child VALUES (5, 7, 1);
CREATE TABLE 05045_m (x UInt8) ENGINE = Merge(currentDatabase(), '^05045_child$');

SELECT '-- a read of nothing but the virtual columns still reports the child name';
SELECT _table FROM 05045_m;
SELECT _table FROM 05045_m SETTINGS enable_analyzer = 0;
SELECT _database = currentDatabase() FROM 05045_m;
SELECT _database = currentDatabase() FROM 05045_m SETTINGS enable_analyzer = 0;
SELECT _database = currentDatabase(), _table FROM 05045_m;

SELECT '-- the values agree with the child pruning';
SELECT count() FROM 05045_m WHERE _table = '05045_child';
SELECT count() FROM 05045_m WHERE _table = '05045_child' SETTINGS enable_analyzer = 0;
SELECT count() FROM 05045_m WHERE _table = '7';

SELECT '-- a read that also needs a real column is not affected';
SELECT _table, x FROM 05045_m;
SELECT _table, x FROM 05045_m SETTINGS enable_analyzer = 0;

SELECT '-- the child column itself is still readable under its own name';
SELECT `_table`, `_database`, x FROM 05045_child;

SELECT '-- a row policy that needs the child column keeps filtering on the child value';
CREATE ROW POLICY 05045_p ON 05045_child USING `_table` > 3 AS PERMISSIVE TO ALL;
SELECT _table, x FROM 05045_m;
SELECT count() FROM 05045_m WHERE _table = '05045_child';
DROP ROW POLICY 05045_p ON 05045_child;
CREATE ROW POLICY 05045_p ON 05045_child USING `_table` < 3 AS PERMISSIVE TO ALL;
SELECT count() FROM 05045_m;
DROP ROW POLICY 05045_p ON 05045_child;

SELECT '-- an ALIAS column over the child column keeps its own value';
CREATE TABLE 05045_alias_child (`_table` UInt8, x UInt8, y UInt8 ALIAS `_table` + 1) ENGINE = MergeTree ORDER BY x;
INSERT INTO 05045_alias_child VALUES (7, 1);
CREATE TABLE 05045_alias_m (x UInt8, y UInt8) ENGINE = Merge(currentDatabase(), '^05045_alias_child$');
SELECT _table, y FROM 05045_alias_m;
SELECT _table, y FROM 05045_alias_m SETTINGS enable_analyzer = 0;
SELECT y FROM 05045_alias_m;

SELECT '-- a child whose only column is named _table leaves the stream with the constant alone';
CREATE TABLE 05045_only_child (`_table` UInt8) ENGINE = MergeTree ORDER BY `_table`;
INSERT INTO 05045_only_child VALUES (7), (8);
CREATE TABLE 05045_only_m (x UInt8) ENGINE = Merge(currentDatabase(), '^05045_only_child$');
SELECT _table FROM 05045_only_m;
SELECT _table FROM 05045_only_m SETTINGS enable_analyzer = 0;
SELECT count() FROM 05045_only_m WHERE _table = '05045_only_child';

SELECT '-- a Merge table that declares _table itself reads the child column instead';
CREATE TABLE 05045_own_m (`_table` UInt8) ENGINE = Merge(currentDatabase(), '^05045_only_child$');
SELECT `_table` FROM 05045_own_m ORDER BY `_table`;
DROP TABLE 05045_own_m;

SELECT '-- a delegating child reports its own name, not the name behind it';
CREATE TABLE 05045_inner (x UInt8) ENGINE = Merge(currentDatabase(), '^05045_child$');
CREATE TABLE 05045_outer (x UInt8) ENGINE = Merge(currentDatabase(), '^05045_inner$');
SELECT _table FROM 05045_outer;
SELECT count() FROM 05045_outer WHERE _table = '05045_inner';
SELECT count() FROM 05045_outer WHERE _table = '05045_child';

DROP TABLE 05045_outer;
DROP TABLE 05045_inner;
DROP TABLE 05045_only_m;
DROP TABLE 05045_only_child;
DROP TABLE 05045_alias_m;
DROP TABLE 05045_alias_child;
DROP TABLE 05045_m;
DROP TABLE 05045_child;
