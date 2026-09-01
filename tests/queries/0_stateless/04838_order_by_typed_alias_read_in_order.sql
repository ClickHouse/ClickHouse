-- An `ALIAS` column is read as its expression converted to the declared type, so `ORDER BY` over a
-- typed `ALIAS` must not be optimized into a read in the sorting-key order of the source column:
-- for `b UInt8 ALIAS a` over `a UInt16` the conversion is not monotonic.

DROP TABLE IF EXISTS t_order_by_typed_alias;

CREATE TABLE t_order_by_typed_alias
(
    a UInt16,
    b UInt8 ALIAS a
)
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;

INSERT INTO t_order_by_typed_alias VALUES (255), (256), (257);

SET optimize_read_in_order = 1;
SET optimize_respect_aliases = 1;

SELECT '-- b is toUInt8(a), so its order differs from the order of a';
SELECT a, b FROM t_order_by_typed_alias ORDER BY a;

SELECT '-- ORDER BY the alias must sort by the alias value';
SELECT a, b FROM t_order_by_typed_alias ORDER BY b, a;
SELECT a, b FROM t_order_by_typed_alias ORDER BY b, a LIMIT 1;
SELECT a, b FROM t_order_by_typed_alias ORDER BY b DESC, a LIMIT 1;

SELECT '-- an alias whose expression already has the declared type still reads in order';
DROP TABLE IF EXISTS t_order_by_same_type_alias;

CREATE TABLE t_order_by_same_type_alias
(
    a UInt16,
    b UInt16 ALIAS a
)
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;

INSERT INTO t_order_by_same_type_alias VALUES (255), (256), (257);

SELECT a, b FROM t_order_by_same_type_alias ORDER BY b LIMIT 1;

DROP TABLE t_order_by_same_type_alias;
DROP TABLE t_order_by_typed_alias;
