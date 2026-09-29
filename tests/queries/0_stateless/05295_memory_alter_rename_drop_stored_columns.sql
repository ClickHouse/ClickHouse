-- RENAME COLUMN and DROP COLUMN on a Memory table: the rows inserted before the ALTER are read under the new
-- names, and a column added later with an old name gets default values, not the old column's data.

DROP TABLE IF EXISTS mem_nullable;
CREATE TABLE mem_nullable (c0 Nullable(UInt8)) ENGINE = Memory;
INSERT INTO mem_nullable VALUES (5), (NULL);
ALTER TABLE mem_nullable RENAME COLUMN c0 TO c1;
SELECT 'rename nullable', c1, c1.null, isNull(c1) FROM mem_nullable ORDER BY c1;
ALTER TABLE mem_nullable ADD COLUMN c0 UInt64;
SELECT 'add old name', c1, c0 FROM mem_nullable ORDER BY c1;
DROP TABLE mem_nullable;

DROP TABLE IF EXISTS mem_string;
CREATE TABLE mem_string (k UInt64, s String, a Array(UInt32)) ENGINE = Memory;
INSERT INTO mem_string VALUES (1, 'hello', [1, 2]), (2, 'world', [3]);
ALTER TABLE mem_string RENAME COLUMN s TO s2;
SELECT 'rename string', k, s2, a FROM mem_string ORDER BY k;
SELECT 'where', count() FROM mem_string WHERE s2 = 'hello';
SELECT 'prewhere', count() FROM mem_string PREWHERE s2 = 'hello';
INSERT INTO mem_string VALUES (3, 'after', [9]);
SELECT 'old and new rows', k, s2 FROM mem_string ORDER BY k;
ALTER TABLE mem_string ADD COLUMN s String;
SELECT 'add old name', k, s2, s FROM mem_string ORDER BY k;
DROP TABLE mem_string;

DROP TABLE IF EXISTS mem_drop;
CREATE TABLE mem_drop (k UInt64, s String) ENGINE = Memory;
INSERT INTO mem_drop VALUES (1, 'hello'), (2, 'world');
ALTER TABLE mem_drop DROP COLUMN s;
ALTER TABLE mem_drop ADD COLUMN s String;
SELECT 'drop then add', k, s FROM mem_drop ORDER BY k;
DROP TABLE mem_drop;

-- The commands of one ALTER are applied in order.
DROP TABLE IF EXISTS mem_multi;
CREATE TABLE mem_multi (a UInt8, b UInt8) ENGINE = Memory;
INSERT INTO mem_multi VALUES (1, 2);
ALTER TABLE mem_multi DROP COLUMN b, RENAME COLUMN a TO b;
SELECT 'drop b, rename a to b', b FROM mem_multi;
DROP TABLE mem_multi;

DROP TABLE IF EXISTS mem_multi;
CREATE TABLE mem_multi (a UInt8, b UInt8) ENGINE = Memory;
INSERT INTO mem_multi VALUES (1, 2);
ALTER TABLE mem_multi RENAME COLUMN b TO c, RENAME COLUMN a TO b;
SELECT 'rename b to c, a to b', b, c FROM mem_multi;
DROP TABLE mem_multi;

DROP TABLE IF EXISTS mem_multi;
CREATE TABLE mem_multi (a UInt8) ENGINE = Memory;
INSERT INTO mem_multi VALUES (1);
ALTER TABLE mem_multi RENAME COLUMN a TO b, ADD COLUMN a UInt8;
SELECT 'rename a to b, add a', b, a FROM mem_multi;
DROP TABLE mem_multi;

DROP TABLE IF EXISTS mem_nested;
CREATE TABLE mem_nested (k UInt8, n Nested(x UInt8, y String)) ENGINE = Memory;
INSERT INTO mem_nested VALUES (1, [1, 2], ['p', 'q']);
ALTER TABLE mem_nested RENAME COLUMN n.x TO n.z;
SELECT 'rename nested member', k, n.z, n.y FROM mem_nested;
-- There is no column `n`, only `n.z` and `n.y`, so nothing is dropped.
ALTER TABLE mem_nested DROP COLUMN IF EXISTS n;
SELECT 'drop if exists nested', k, n.z, n.y FROM mem_nested;
ALTER TABLE mem_nested DROP COLUMN n;
ALTER TABLE mem_nested ADD COLUMN n Nested(z UInt8, y String);
SELECT 'drop then add nested', k, n.z, n.y FROM mem_nested;
DROP TABLE mem_nested;

DROP TABLE IF EXISTS mem_update;
CREATE TABLE mem_update (k UInt8, x UInt8) ENGINE = Memory;
INSERT INTO mem_update VALUES (1, 5), (2, 6);
ALTER TABLE mem_update RENAME COLUMN x TO y;
ALTER TABLE mem_update UPDATE y = 9 WHERE k = 1;
SELECT 'rename then update', k, y FROM mem_update ORDER BY k;
DROP TABLE mem_update;

DROP TABLE IF EXISTS mem_compress;
CREATE TABLE mem_compress (k UInt64, s String) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO mem_compress VALUES (1, 'hello'), (2, 'world');
ALTER TABLE mem_compress RENAME COLUMN s TO s2;
SELECT 'compress', k, s2 FROM mem_compress ORDER BY k;
DROP TABLE mem_compress;

CREATE TEMPORARY TABLE tmp_rename (k UInt64, s String) ENGINE = Memory;
INSERT INTO tmp_rename VALUES (1, 'hello');
ALTER TABLE tmp_rename RENAME COLUMN s TO s2;
SELECT 'temporary table', k, s2 FROM tmp_rename;
DROP TEMPORARY TABLE tmp_rename;

-- The rows inserted before `b` was added have no stored column left after `a` is dropped, and keep their count.
DROP TABLE IF EXISTS mem_fill;
CREATE TABLE mem_fill (a UInt8) ENGINE = Memory;
INSERT INTO mem_fill VALUES (1), (2), (3);
ALTER TABLE mem_fill ADD COLUMN b Nullable(UInt8);
ALTER TABLE mem_fill DROP COLUMN a;
SELECT 'all stored columns dropped', count(), countIf(b IS NULL), groupArray(b) FROM mem_fill;
SELECT 'total_rows', total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'mem_fill';
INSERT INTO mem_fill (b) VALUES (5);
SELECT 'insert after drop', count(), countIf(b IS NULL), sum(b) FROM mem_fill;
ALTER TABLE mem_fill ADD COLUMN a UInt8;
SELECT 'add dropped name', a, b FROM mem_fill ORDER BY b NULLS FIRST;
DROP TABLE mem_fill;

CREATE TEMPORARY TABLE tmp_last (a UInt8, e UInt8 ALIAS 7) ENGINE = Memory;
INSERT INTO tmp_last VALUES (1), (2);
ALTER TABLE tmp_last DROP COLUMN a; -- { serverError EMPTY_LIST_OF_COLUMNS_PASSED }
SELECT 'rejected drop of the last physical column', a, e FROM tmp_last ORDER BY a;
DROP TEMPORARY TABLE tmp_last;

-- The data of a dropped column is released.
DROP TABLE IF EXISTS mem_bytes;
CREATE TABLE mem_bytes (k UInt8, s String) ENGINE = Memory;
INSERT INTO mem_bytes SELECT toUInt8(number), repeat('x', 100) FROM numbers(10000) SETTINGS max_block_size = 65536;
SELECT 'bytes before drop', total_bytes > 900000 FROM system.tables WHERE database = currentDatabase() AND name = 'mem_bytes';
ALTER TABLE mem_bytes DROP COLUMN s;
SELECT 'bytes after drop', total_bytes < 200000 FROM system.tables WHERE database = currentDatabase() AND name = 'mem_bytes';
DROP TABLE mem_bytes;

-- With `compress = 1` the column that keeps the row count of a block without stored columns is compressed as well.
DROP TABLE IF EXISTS mem_compress_fill;
CREATE TABLE mem_compress_fill (a UInt64) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO mem_compress_fill SELECT number FROM numbers(100000) SETTINGS max_block_size = 100000;
ALTER TABLE mem_compress_fill ADD COLUMN b UInt64;
ALTER TABLE mem_compress_fill DROP COLUMN a;
SELECT 'compressed fill', count(), sum(b) FROM mem_compress_fill;
SELECT 'compressed fill bytes', total_bytes < 100000 FROM system.tables WHERE database = currentDatabase() AND name = 'mem_compress_fill';
DROP TABLE mem_compress_fill;

-- A `MODIFY SETTING` in the same ALTER as other commands takes effect as well.
DROP TABLE IF EXISTS mem_mixed_alter;
CREATE TABLE mem_mixed_alter (a UInt64) ENGINE = Memory;
INSERT INTO mem_mixed_alter SELECT number FROM numbers(100000) SETTINGS max_block_size = 100000;
ALTER TABLE mem_mixed_alter ADD COLUMN b UInt64, DROP COLUMN a, MODIFY SETTING compress = 1;
SELECT 'mixed alter', count(), sum(b) FROM mem_mixed_alter;
SELECT 'mixed alter bytes', total_bytes < 100000 FROM system.tables WHERE database = currentDatabase() AND name = 'mem_mixed_alter';
DROP TABLE mem_mixed_alter;

-- A rejected ALTER does not apply its settings; an accepted one removes the oldest rows beyond `max_rows_to_keep`, now and on later inserts.
CREATE TEMPORARY TABLE tmp_mixed (a UInt8, e UInt8 ALIAS 7) ENGINE = Memory;
INSERT INTO tmp_mixed SELECT 1;
INSERT INTO tmp_mixed SELECT 2;
INSERT INTO tmp_mixed SELECT 3;
ALTER TABLE tmp_mixed DROP COLUMN a, MODIFY SETTING max_rows_to_keep = 1; -- { serverError EMPTY_LIST_OF_COLUMNS_PASSED }
SELECT 'rejected mixed alter', count(), sum(a) FROM tmp_mixed;
ALTER TABLE tmp_mixed ADD COLUMN b UInt8, MODIFY SETTING max_rows_to_keep = 1;
SELECT 'accepted mixed alter', count(), sum(a) FROM tmp_mixed;
INSERT INTO tmp_mixed (a) SELECT 4;
SELECT 'insert after mixed alter', count(), sum(a) FROM tmp_mixed;
DROP TEMPORARY TABLE tmp_mixed;

-- A restored column that the table does not have is not kept: the restored rows keep their count, and a column added
-- later with its name gets default values.
DROP TABLE IF EXISTS mem_backup_src;
DROP TABLE IF EXISTS mem_restored;
CREATE TABLE mem_backup_src (a UInt64) ENGINE = Memory;
INSERT INTO mem_backup_src VALUES (7), (9);
BACKUP TABLE mem_backup_src TO Memory('05295_mem_backup') FORMAT Null;
CREATE TABLE mem_restored (k UInt64) ENGINE = Memory;
RESTORE TABLE mem_backup_src AS mem_restored FROM Memory('05295_mem_backup') SETTINGS allow_different_table_def = 1 FORMAT Null;
ALTER TABLE mem_restored ADD COLUMN a UInt64;
SELECT 'restored column the table lacks', count(), sum(k), sum(a) FROM mem_restored;
DROP TABLE mem_backup_src;
DROP TABLE mem_restored;

-- Rows whose only stored column is an empty `Tuple()` take no bytes: BACKUP keeps them, and RESTORE does not see
-- the table as empty.
DROP TABLE IF EXISTS mem_empty_tuple;
CREATE TABLE mem_empty_tuple (a UInt8) ENGINE = Memory;
INSERT INTO mem_empty_tuple VALUES (1), (2), (3);
ALTER TABLE mem_empty_tuple ADD COLUMN e Tuple();
ALTER TABLE mem_empty_tuple DROP COLUMN a;
SELECT 'empty tuple rows', total_rows, total_bytes FROM system.tables WHERE database = currentDatabase() AND name = 'mem_empty_tuple';
BACKUP TABLE mem_empty_tuple TO Memory('05295_empty_tuple_backup') FORMAT Null;
DROP TABLE mem_empty_tuple SYNC;
RESTORE TABLE mem_empty_tuple FROM Memory('05295_empty_tuple_backup') FORMAT Null;
SELECT 'empty tuple restored', count() FROM mem_empty_tuple;
RESTORE TABLE mem_empty_tuple FROM Memory('05295_empty_tuple_backup') FORMAT Null; -- { serverError CANNOT_RESTORE_TABLE }
SELECT 'empty tuple restore into a non-empty table', count() FROM mem_empty_tuple;
DROP TABLE mem_empty_tuple;

-- BACKUP checks that the table definition and the data have the same stored columns: ALIAS and EPHEMERAL columns are
-- not stored, MATERIALIZED and Nested ones are.
DROP TABLE IF EXISTS mem_column_kinds;
CREATE TABLE mem_column_kinds (k UInt8, m UInt8 MATERIALIZED k + 1, al UInt8 ALIAS k + 2, ep UInt8 EPHEMERAL, n Nested(x UInt8)) ENGINE = Memory;
INSERT INTO mem_column_kinds (k, `n.x`) VALUES (1, [5]);
BACKUP TABLE mem_column_kinds TO Memory('05295_column_kinds_backup') FORMAT Null;
DROP TABLE mem_column_kinds SYNC;
RESTORE TABLE mem_column_kinds FROM Memory('05295_column_kinds_backup') FORMAT Null;
SELECT 'column kinds restored', k, m, al, n.x FROM mem_column_kinds;
DROP TABLE mem_column_kinds;
