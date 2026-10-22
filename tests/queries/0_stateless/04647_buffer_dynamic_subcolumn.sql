-- Reading a dynamic subcolumn (a path inside `JSON`) through a `Buffer` table.

-- The first half of the test uses a `Buffer` table whose structure differs from the destination on purpose,
-- and `StorageBuffer` reports every such difference with a warning - do not send these warnings to the client.
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS t_buffer_dynamic_subcolumn_dst;
DROP TABLE IF EXISTS t_buffer_dynamic_subcolumn;

-- The destination has a typed path, while the `Buffer` table does not - the structures are different.
CREATE TABLE t_buffer_dynamic_subcolumn_dst (c0 JSON(a UInt32)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_buffer_dynamic_subcolumn (c0 JSON)
    ENGINE = Buffer(currentDatabase(), t_buffer_dynamic_subcolumn_dst, 1, 1000, 1000, 1000, 1000, 1000000, 1000000);

INSERT INTO t_buffer_dynamic_subcolumn_dst VALUES ('{"a":7}');
INSERT INTO t_buffer_dynamic_subcolumn VALUES ('{"a":5}');

SELECT toTypeName(c0.a), c0.a FROM t_buffer_dynamic_subcolumn ORDER BY toString(c0.a);
SELECT c0.a FROM t_buffer_dynamic_subcolumn WHERE c0.a = 7;
SELECT count() FROM t_buffer_dynamic_subcolumn WHERE notLike(toString(c0.a), 'nothing%');

DROP TABLE t_buffer_dynamic_subcolumn;
DROP TABLE t_buffer_dynamic_subcolumn_dst;

-- The same structure on both sides: the data from the destination must not be lost.
CREATE TABLE t_buffer_dynamic_subcolumn_dst (c0 JSON) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_buffer_dynamic_subcolumn (c0 JSON)
    ENGINE = Buffer(currentDatabase(), t_buffer_dynamic_subcolumn_dst, 1, 1000, 1000, 1000, 1000, 1000000, 1000000);

INSERT INTO t_buffer_dynamic_subcolumn_dst VALUES ('{"a":11}');
INSERT INTO t_buffer_dynamic_subcolumn VALUES ('{"a":1}');

SELECT toTypeName(c0.a), c0.a FROM t_buffer_dynamic_subcolumn ORDER BY toString(c0.a);

DROP TABLE t_buffer_dynamic_subcolumn;
DROP TABLE t_buffer_dynamic_subcolumn_dst;
