SET cast_keep_nullable = 1;

DROP TABLE IF EXISTS t_dynamic_null;
CREATE TABLE t_dynamic_null (d Dynamic) ENGINE = Memory;
INSERT INTO t_dynamic_null VALUES (1), (NULL), (3);

SELECT '-- CAST on Dynamic with NULL:';
SELECT d::Float64 AS r, toTypeName(r) FROM t_dynamic_null ORDER BY 1 NULLS LAST;

SELECT '-- toFloat64 on Dynamic with NULL:';
SELECT toFloat64(d) AS r, toTypeName(r) FROM t_dynamic_null ORDER BY 1 NULLS LAST;

SELECT '-- toUInt32 on Dynamic with NULL:';
SELECT toUInt32(d) AS r, toTypeName(r) FROM t_dynamic_null ORDER BY 1 NULLS LAST;

SELECT '-- toInt8 on Dynamic with NULL:';
SELECT toInt8(d) AS r, toTypeName(r) FROM t_dynamic_null ORDER BY 1 NULLS LAST;

SELECT '-- toString on Dynamic with NULL:';
SELECT toString(d) AS r, toTypeName(r) FROM t_dynamic_null ORDER BY 1 NULLS LAST;

DROP TABLE t_dynamic_null;

SELECT '-- Dynamic without cast_keep_nullable (backward compatible):';
SET cast_keep_nullable = 0;

DROP TABLE IF EXISTS t_dynamic_no_keep;
CREATE TABLE t_dynamic_no_keep (d Dynamic) ENGINE = Memory;
INSERT INTO t_dynamic_no_keep VALUES (1), (NULL), (3);

SELECT '-- toFloat64/toString return non-Nullable with defaults for NULL:';
SELECT toFloat64(d) AS r, toTypeName(r) FROM t_dynamic_no_keep ORDER BY 1;
SELECT toString(d) AS r, toTypeName(r) FROM t_dynamic_no_keep ORDER BY 1;

SELECT '-- CAST returns non-Nullable with defaults for NULL:';
SELECT d::Float64 AS r, toTypeName(r) FROM t_dynamic_no_keep ORDER BY 1;
SELECT d::String AS r, toTypeName(r) FROM t_dynamic_no_keep ORDER BY 1;

DROP TABLE t_dynamic_no_keep;
