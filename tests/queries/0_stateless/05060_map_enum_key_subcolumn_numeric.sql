-- Element access of a Map with Enum keys by the numeric value of the enum.
-- A value that is not in the Enum has no name, hence no key subcolumn, and must not be
-- rewritten to one: it reads as the default value, like a key that is missing from the map.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_map_enum_numeric;

CREATE TABLE t_map_enum_numeric
(
    id UInt64,
    m Map(Enum8('a' = 1, 'b' = 2, 'c' = 3), Int64)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_map_enum_numeric VALUES (1, map('a', 10, 'c', 30)), (2, map('b', 20));

SELECT m[1], m[2], m[3] FROM t_map_enum_numeric ORDER BY id;

-- A value outside of the Enum reads as the default value instead of throwing.
SELECT m[toInt8(4)], m[toInt8(-5)] FROM t_map_enum_numeric ORDER BY id;

-- Access by the numeric value is rewritten to the same key subcolumn as access by the name.
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT m[1] FROM t_map_enum_numeric) WHERE explain LIKE '%key\_a%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT m[toInt8(4)] FROM t_map_enum_numeric) WHERE explain LIKE '%key\_%';

DROP TABLE t_map_enum_numeric;
