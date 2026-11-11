-- Tags: memory-engine

-- ALTER TABLE ... UPDATE and MATERIALIZE COLUMN of a Memory table whose blocks were inserted before
-- MODIFY COLUMN of the updated column.

DROP TABLE IF EXISTS t_modify;
CREATE TABLE t_modify (k UInt64, x UInt8) ENGINE = Memory;
INSERT INTO t_modify VALUES (1, 5), (2, 6);
ALTER TABLE t_modify MODIFY COLUMN x String;
INSERT INTO t_modify VALUES (3, '7');
ALTER TABLE t_modify UPDATE x = '42' WHERE k = 1;
SELECT 'modify, update', k, x FROM t_modify ORDER BY k;
ALTER TABLE t_modify MODIFY COLUMN x UInt8;
SELECT 'modify back', k, x, x + 1 FROM t_modify ORDER BY k;
BACKUP TABLE t_modify TO Memory('05301_backup') FORMAT Null;
DROP TABLE t_modify SYNC;
RESTORE TABLE t_modify FROM Memory('05301_backup') FORMAT Null;
SELECT 'restored', k, x FROM t_modify ORDER BY k;
DROP TABLE t_modify;

DROP TABLE IF EXISTS t_nullable;
CREATE TABLE t_nullable (k UInt64, x Nullable(UInt8)) ENGINE = Memory;
INSERT INTO t_nullable VALUES (1, 5), (2, NULL);
ALTER TABLE t_nullable MODIFY COLUMN x Nullable(String);
ALTER TABLE t_nullable UPDATE x = 'a' WHERE k = 1;
SELECT 'nullable', k, x, x.null FROM t_nullable ORDER BY k;
DROP TABLE t_nullable;

DROP TABLE IF EXISTS t_compress;
CREATE TABLE t_compress (k UInt64, x UInt8) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t_compress VALUES (1, 5), (2, 6);
ALTER TABLE t_compress MODIFY COLUMN x String;
ALTER TABLE t_compress UPDATE x = 'abc' WHERE k = 1;
SELECT 'compress', k, x FROM t_compress ORDER BY k;
DROP TABLE t_compress;

DROP TABLE IF EXISTS t_materialize;
CREATE TABLE t_materialize (k UInt64, d UInt8 MATERIALIZED 1) ENGINE = Memory;
INSERT INTO t_materialize (k) VALUES (1), (2);
ALTER TABLE t_materialize MODIFY COLUMN d String MATERIALIZED concat('d', toString(k));
ALTER TABLE t_materialize MATERIALIZE COLUMN d;
SELECT 'materialize', k, d FROM t_materialize ORDER BY k;
DROP TABLE t_materialize;
