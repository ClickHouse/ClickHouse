SET enable_analyzer = 1;
SET use_variant_as_common_type = 1;

-- Reproducer: empty tuple vs non-empty tuple in INTERSECT ALL
SELECT count() FROM
(
    SELECT if(1, tuple(), (NULL, 2147483647, 1, 10)) AS k
    INTERSECT ALL
    SELECT if(0, (-2147483648, -1), toNullable((NULL, NULL, 2147483646, 0))) AS k
);

-- Direct if() with empty tuple vs non-empty tuple (Variant result)
SELECT toTypeName(if(1, tuple(), (NULL, CAST(2147483647, 'UInt32'), CAST(1, 'UInt8'), CAST(10, 'UInt8'))));

-- Empty tuple vs empty tuple (should still work)
SELECT if(1, tuple(), tuple());

-- Same-arity tuples (regression guard)
SELECT toTypeName(if(1, CAST((1, 2), 'Tuple(UInt8, UInt8)'), CAST((3, 4), 'Tuple(UInt8, UInt8)')));

-- Array containing empty and non-empty tuples (Variant)
SELECT toTypeName([tuple(), CAST((1, 2), 'Tuple(UInt8, UInt8)')]);

-- Without Variant: should error on incompatible tuple arities
SELECT [tuple(), (1, 2)] SETTINGS use_variant_as_common_type = 0; -- { serverError NO_COMMON_TYPE }

-- Legacy path: should error on incompatible tuple arities (no Variant in legacy INTERSECT)
SELECT 1 FROM (SELECT tuple() AS k INTERSECT ALL SELECT (1, 2) AS k) SETTINGS enable_analyzer = 0, use_variant_as_common_type = 0; -- { serverError NO_COMMON_TYPE }
