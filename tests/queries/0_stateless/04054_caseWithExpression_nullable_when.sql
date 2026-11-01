-- Test that CASE expression handles Nullable WHEN values gracefully
-- when the expression itself is non-Nullable. Previously, the `transform`
-- function would fail trying to cast Nullable WHEN values to the
-- non-Nullable expression type.

-- Nullable WHEN value with non-Nullable expression
SELECT CASE 33 WHEN CAST(NULL AS Nullable(INTEGER)) THEN 1 ELSE 2 END;

-- NULL literal as WHEN value
SELECT CASE 33 WHEN NULL THEN 1 ELSE 2 END;

-- NULL in ELSE branch with Nullable WHEN
SELECT CASE 33 WHEN CAST(NULL AS Nullable(INTEGER)) THEN 1 ELSE NULL END;

-- Multiple WHEN branches, one with NULL
SELECT CASE 10 WHEN NULL THEN 1 WHEN 10 THEN 2 ELSE 3 END;

-- With cast_keep_nullable, CAST(NULL AS INTEGER) becomes Nullable
SET cast_keep_nullable = 1;
SELECT CASE 33 WHEN CAST(NULL AS INTEGER) THEN 1 ELSE 2 END;
SET cast_keep_nullable = 0;

-- Mixed types that trigger NO_COMMON_TYPE fallback to multiIf
SELECT CASE -14 WHEN 62.75 THEN 1 WHEN -80 THEN 2 ELSE 3 END;

-- Normal CASE still uses optimized transform path
SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END;
SELECT CASE 2 WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE 30 END;
