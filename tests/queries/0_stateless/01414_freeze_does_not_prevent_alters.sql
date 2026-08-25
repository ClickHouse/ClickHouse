-- In previous ClickHouse versions, parts were not 100% immutable and FREEZE may prevent subsequent ALTERs.
-- It's not longer the case. Let's prove it.

DROP TABLE IF EXISTS t;
CREATE TABLE t (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t VALUES (1, 'hello'), (2, 'world');

SELECT * FROM t;
SELECT name, is_frozen FROM system.parts WHERE database = currentDatabase() AND table = 't';

SELECT '---';
ALTER TABLE t FREEZE;
SELECT name, is_frozen FROM system.parts WHERE database = currentDatabase() AND table = 't';

SELECT '---';
SET mutations_sync = 1;
ALTER TABLE t UPDATE s = 'goodbye' WHERE k = 1;
SELECT * FROM t;
SELECT name, is_frozen FROM system.parts WHERE database = currentDatabase() AND table = 't';

SELECT '---';
ALTER TABLE t MODIFY COLUMN s Enum('goodbye' = 1, 'world' = 2);
SELECT * FROM t;
SELECT name, is_frozen FROM system.parts WHERE database = currentDatabase() AND table = 't';

SELECT '---';
ALTER TABLE t FREEZE;
SELECT name, is_frozen FROM system.parts WHERE database = currentDatabase() AND table = 't';

SELECT '---';
ALTER TABLE t MODIFY COLUMN s Enum('hello' = 1, 'world' = 2);
SELECT * FROM t;
SELECT name, is_frozen FROM system.parts WHERE database = currentDatabase() AND table = 't';

DROP TABLE t;
