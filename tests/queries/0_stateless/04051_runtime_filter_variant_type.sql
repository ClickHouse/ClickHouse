-- Reproducer for a bug where __applyFilter runtime filter returned Nullable(UInt8) instead of UInt8
-- when joining on Variant columns. The Variant type can represent NULL, but `hasNullable` returns
-- false for it, so the runtime filter incorrectly chose the single-element `equals` optimization
-- (ValuesCount::ONE path) which produces Nullable(UInt8) via the Variant function adaptor.
--
-- The right side must have exactly 1 row to trigger the ONE code path.

SET enable_analyzer = 1;

-- Minimal reproducer: join on a Variant column with 1 row on the right side
SELECT *
FROM
    (SELECT v FROM format(TSV, 'v Variant(String, Bool)', 'true\nfalse')) AS t1
    JOIN
    (SELECT v FROM format(TSV, 'v Variant(String, Bool)', 'true')) AS t2
    USING (v);

-- Same but with NULL values on the left side (NULL should not match)
SELECT *
FROM
    (SELECT v FROM format(TSV, 'v Variant(String, Bool)', 'true\n\\N\nfalse')) AS t1
    JOIN
    (SELECT v FROM format(TSV, 'v Variant(String, Bool)', 'true')) AS t2
    USING (v);
