-- `UUID` and `UUID2` constants share the `Field` representation. Converting such a constant to `Variant` or `Dynamic`
-- through the `Field` path (`VALUES` without template deduction, the `values` table function) must keep the value.

SET input_format_values_deduce_templates_of_expressions = 0;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_variant;
CREATE TABLE t_variant (id UInt8, v Variant(UUID2, String)) ENGINE = Memory;
INSERT INTO t_variant VALUES (1, toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'));
INSERT INTO t_variant VALUES (2, toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0'));
SELECT id, v, variantType(v) FROM t_variant ORDER BY id;
DROP TABLE t_variant;

CREATE TABLE t_variant (id UInt8, v Variant(UUID, String)) ENGINE = Memory;
INSERT INTO t_variant VALUES (1, toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'));
INSERT INTO t_variant VALUES (2, toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0'));
SELECT id, v, variantType(v) FROM t_variant ORDER BY id;
DROP TABLE t_variant;

CREATE TABLE t_variant (id UInt8, v Variant(Array(UUID), String)) ENGINE = Memory;
INSERT INTO t_variant VALUES (1, [toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0')]);
SELECT id, v, variantType(v) FROM t_variant ORDER BY id;
DROP TABLE t_variant;

-- A `UUID2` constant cannot reach the `UUID2` alternative when there is also a `UUID` one.
CREATE TABLE t_variant (id UInt8, v Variant(UUID, UUID2)) ENGINE = Memory;
INSERT INTO t_variant VALUES (1, toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'));
INSERT INTO t_variant VALUES (2, toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0')); -- { error TYPE_MISMATCH }
SELECT id, v, variantType(v) FROM t_variant ORDER BY id;
DROP TABLE t_variant;

-- The `values` table function converts the constants through the `Field` path as well.
SELECT d, dynamicType(d) FROM values('d Dynamic', toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), [toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0')], tuple(toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0')));
SELECT v, variantType(v) FROM values('v Variant(UUID, String)', toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0'));
SELECT v, variantType(v) FROM values('v Variant(UUID2, String)', toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'));
SELECT v, variantType(v) FROM values('v Variant(UUID, UUID2)', toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'));
SELECT v, variantType(v) FROM values('v Variant(UUID, UUID2)', toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0')); -- { serverError TYPE_MISMATCH }

-- A non-NULL value of a `Nullable(UUID2)` or `LowCardinality(UUID2)` source is a `UUID2` value as well.
CREATE TABLE t_variant (id UInt8, v Variant(UUID, UUID2)) ENGINE = Memory;
INSERT INTO t_variant VALUES (1, toNullable(toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))); -- { error TYPE_MISMATCH }
INSERT INTO t_variant VALUES (2, toLowCardinality(toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))); -- { error TYPE_MISMATCH }
SELECT count() FROM t_variant;
DROP TABLE t_variant;

CREATE TABLE t_variant (id UInt8, v Variant(UUID2, String)) ENGINE = Memory;
INSERT INTO t_variant VALUES (1, toNullable(toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0')));
INSERT INTO t_variant VALUES (2, toLowCardinality(toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0')));
SELECT id, v, variantType(v) FROM t_variant ORDER BY id;
DROP TABLE t_variant;
