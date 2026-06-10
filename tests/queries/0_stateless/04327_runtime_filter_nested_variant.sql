-- Runtime filter on a join key that contains a Variant/Dynamic nested inside a compound
-- type (Tuple/Array/Map). A Variant can be NULL without a Nullable wrapper, but the nested
-- case was not detected, so the single-element "equals" runtime filter path was chosen and
-- produced Nullable(UInt8), tripping the __applyFilter return-type assertion.

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET allow_suspicious_types_in_order_by = 1;

-- Tuple(Variant, ...), exactly 1 distinct value on the right (the "equals" path).
SELECT *
FROM
    (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Variant(String, Bool)', 'true\nfalse')) AS t1
    JOIN
    (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Variant(String, Bool)', 'true')) AS t2
    USING (k)
ORDER BY k;

-- Tuple(Variant, ...), several distinct values on the right (the set-lookup path).
SELECT *
FROM
    (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Variant(String, Bool)', 'true\nfalse\nstr')) AS t1
    JOIN
    (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Variant(String, Bool)', 'true\nstr')) AS t2
    USING (k)
ORDER BY k;

-- Array(Variant) as the join key.
SELECT *
FROM
    (SELECT [v] AS k FROM format(TSV, 'v Variant(String, Bool)', 'true\nfalse')) AS t1
    JOIN
    (SELECT [v] AS k FROM format(TSV, 'v Variant(String, Bool)', 'true')) AS t2
    USING (k)
ORDER BY k;

-- Map(String, Variant) as the join key.
SELECT *
FROM
    (SELECT map('a', v) AS k FROM format(TSV, 'v Variant(String, Bool)', 'true\nfalse')) AS t1
    JOIN
    (SELECT map('a', v) AS k FROM format(TSV, 'v Variant(String, Bool)', 'true')) AS t2
    USING (k)
ORDER BY k;
