SET enable_variant_type = 1;
SET allow_suspicious_variant_types = 1;
SET cast_keep_nullable = 1;

SELECT '-- Variant with cast_keep_nullable, single type and NULLs:';
DROP TABLE IF EXISTS t_variant_null;
CREATE TABLE t_variant_null (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO t_variant_null VALUES (1), (NULL), (3);

SELECT '-- toFloat64:';
SELECT toFloat64(v) AS r, toTypeName(r) FROM t_variant_null ORDER BY 1 NULLS LAST;

SELECT '-- CAST to Float64:';
SELECT v::Float64 AS r, toTypeName(r) FROM t_variant_null ORDER BY 1 NULLS LAST;

SELECT '-- toUInt32:';
SELECT toUInt32(v) AS r, toTypeName(r) FROM t_variant_null ORDER BY 1 NULLS LAST;

SELECT '-- toInt8:';
SELECT toInt8(v) AS r, toTypeName(r) FROM t_variant_null ORDER BY 1 NULLS LAST;

SELECT '-- toString:';
SELECT toString(v) AS r, toTypeName(r) FROM t_variant_null ORDER BY 1 NULLS LAST;

SELECT '-- CAST to String:';
SELECT v::String AS r, toTypeName(r) FROM t_variant_null ORDER BY 1 NULLS LAST;

DROP TABLE t_variant_null;

SELECT '-- Variant with multiple types and NULLs:';
DROP TABLE IF EXISTS t_variant_multi;
CREATE TABLE t_variant_multi (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO t_variant_multi VALUES (1), (NULL), ('hello'), (42), (NULL);

SELECT '-- toString on multiple variant types:';
SELECT toString(v) AS r, toTypeName(r) FROM t_variant_multi ORDER BY 1 NULLS LAST;

DROP TABLE t_variant_multi;

SELECT '-- Variant single type no NULLs (optimization path):';
DROP TABLE IF EXISTS t_variant_single;
CREATE TABLE t_variant_single (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO t_variant_single VALUES (1), (2), (3);

SELECT toFloat64(v) AS r, toTypeName(r) FROM t_variant_single ORDER BY 1;
SELECT toString(v) AS r, toTypeName(r) FROM t_variant_single ORDER BY 1;

DROP TABLE t_variant_single;

SELECT '-- Variant all NULLs (optimization path):';
DROP TABLE IF EXISTS t_variant_all_null;
CREATE TABLE t_variant_all_null (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO t_variant_all_null VALUES (NULL), (NULL);

SELECT toFloat64(v) AS r, toTypeName(r) FROM t_variant_all_null;
SELECT toString(v) AS r, toTypeName(r) FROM t_variant_all_null;

DROP TABLE t_variant_all_null;

SELECT '-- Variant without cast_keep_nullable:';
SET cast_keep_nullable = 0;

DROP TABLE IF EXISTS t_variant_no_keep;
CREATE TABLE t_variant_no_keep (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO t_variant_no_keep VALUES (1), (NULL), (3);

SELECT '-- toFloat64/toString always wrap in Nullable:';
SELECT toFloat64(v) AS r, toTypeName(r) FROM t_variant_no_keep ORDER BY 1 NULLS LAST;
SELECT toString(v) AS r, toTypeName(r) FROM t_variant_no_keep ORDER BY 1 NULLS LAST;

SELECT '-- CAST throws on NULL without cast_keep_nullable:';
SELECT v::Float64 AS r, toTypeName(r) FROM t_variant_no_keep ORDER BY 1 NULLS LAST; -- {serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN}
SELECT v::String AS r, toTypeName(r) FROM t_variant_no_keep ORDER BY 1 NULLS LAST; -- {serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN}

DROP TABLE t_variant_no_keep;
