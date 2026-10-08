-- A column may be named like a subcolumn of another column, and a name lookup answers with the
-- column. Listing both under that one name was harmless only while their types matched: after
-- `ALTER TABLE ... MODIFY COLUMN` of the shadowing column, building a block of all columns and
-- subcolumns hit two different structures under the name `a.size0` and every read of the table
-- failed with `AMBIGUOUS_COLUMN_NAME`.

DROP TABLE IF EXISTS t_shadowed_size0;
CREATE TABLE t_shadowed_size0 (id UInt64, `a.size0` UInt64, a Array(UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_shadowed_size0 SELECT number, number, [number, number] FROM numbers(10);
ALTER TABLE t_shadowed_size0 MODIFY COLUMN `a.size0` Nullable(UInt64);
INSERT INTO t_shadowed_size0 SELECT number + 10, number, [number] FROM numbers(10);

SELECT 'count of parts of both types';
SELECT count() FROM t_shadowed_size0 WHERE id >= 0;
SELECT count() FROM t_shadowed_size0;

SELECT 'the name answers with the column, not with the size subcolumn';
SELECT id, `a.size0`, a FROM t_shadowed_size0 ORDER BY id LIMIT 2;
SELECT id, `a.size0`, a FROM t_shadowed_size0 ORDER BY id DESC LIMIT 2;

SELECT 'the array itself is still readable';
SELECT sum(length(a)) FROM t_shadowed_size0;

SELECT 'and the column keeps its own values after the type change';
SELECT sum(assumeNotNull(`a.size0`)) FROM t_shadowed_size0;

DROP TABLE t_shadowed_size0;

-- The same shape with an `ALIAS` shadowing a `Map` subcolumn: there the subcolumn is the physical
-- one and the alias is not, so a physical read still finds the subcolumn.
DROP TABLE IF EXISTS t_shadowed_map_keys;
CREATE TABLE t_shadowed_map_keys
(
    id UInt64,
    m Map(String, UInt64),
    `m.keys` Array(String) ALIAS mapKeys(m)
) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_shadowed_map_keys VALUES (1, map('x', 1)), (2, map('y', 2, 'z', 3));

SELECT 'the alias over a map subcolumn';
SELECT id, `m.keys` FROM t_shadowed_map_keys ORDER BY id;
SELECT sum(length(m.keys)) FROM t_shadowed_map_keys;

DROP TABLE t_shadowed_map_keys;
