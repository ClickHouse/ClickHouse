-- The NULL-skipping of Variant arguments must happen outside the combinators, exactly where the
-- Null combinator sits for Nullable arguments. A combinator whose `add` keeps per-row state of its
-- own (the -ArgMin/-ArgMax key, the -OrNull/-OrDefault "there was a value" flag) must not observe
-- the skipped rows: the higher-key NULL row below must not steal the key from the lower-key
-- non-NULL row, and an all-NULL group must count as empty for -OrNull/-OrDefault.

SET allow_experimental_variant_type = 1;

SELECT '-- ArgMax: a higher-key NULL row must not shadow a lower-key non-NULL row';
SELECT anyArgMax(v, k) FROM (SELECT * FROM values('v Variant(String), k UInt64', (NULL, 10), ('x', 5)));
SELECT anyArgMin(v, k) FROM (SELECT * FROM values('v Variant(String), k UInt64', (NULL, 1), ('x', 5)));
SELECT countArgMax(v, k) FROM (SELECT * FROM values('v Variant(String), k UInt64', (NULL, 10), ('x', 5)));
SELECT groupArrayArgMax(v, k) FROM (SELECT * FROM values('v Variant(String), k UInt64', (NULL, 10), ('x', 5), ('y', 5)));
SELECT uniqArgMax(v, k) FROM (SELECT * FROM values('v Variant(String), k UInt64', (NULL, 10), ('x', 5)));

SELECT '-- the same rows over Nullable must agree';
SELECT anyArgMax(v, k) FROM (SELECT * FROM values('v Nullable(String), k UInt64', (NULL, 10), ('x', 5)));
SELECT anyArgMin(v, k) FROM (SELECT * FROM values('v Nullable(String), k UInt64', (NULL, 1), ('x', 5)));
SELECT countArgMax(v, k) FROM (SELECT * FROM values('v Nullable(String), k UInt64', (NULL, 10), ('x', 5)));
SELECT groupArrayArgMax(v, k) FROM (SELECT * FROM values('v Nullable(String), k UInt64', (NULL, 10), ('x', 5), ('y', 5)));
SELECT uniqArgMax(v, k) FROM (SELECT * FROM values('v Nullable(String), k UInt64', (NULL, 10), ('x', 5)));

SELECT '-- nested combinator chains filter once, outermost';
SELECT anyIfArgMax(v, cond, k) FROM (SELECT * FROM values('v Variant(String), cond Bool, k UInt64', (NULL, true, 10), ('x', true, 5)));
SELECT anyArgMaxIf(v, k, cond) FROM (SELECT * FROM values('v Variant(String), k UInt64, cond Bool', (NULL, 10, true), ('x', 5, true)));

SELECT '-- an all-NULL group is empty for -OrNull/-OrDefault, as for Nullable';
SELECT countOrNull(v) FROM (SELECT * FROM values('v Variant(String)', (NULL), (NULL)));
SELECT countOrNull(v) FROM (SELECT * FROM values('v Nullable(String)', (NULL), (NULL)));

SELECT '-- a state written with the key untouched by NULL rows merges the same way';
SELECT anyArgMaxMerge(s) FROM
(
    SELECT anyArgMaxState(v, k) AS s FROM (SELECT * FROM values('v Variant(String), k UInt64', (NULL, 10), ('x', 5))) GROUP BY k
);

SELECT '-- the setting restores the previous behavior';
SELECT anyArgMax(v, k) FROM (SELECT * FROM values('v Variant(String), k UInt64', (NULL, 10), ('x', 5))) SETTINGS aggregate_functions_skip_variant_nulls = 0;
SELECT countOrNull(v) FROM (SELECT * FROM values('v Variant(String)', (NULL), (NULL))) SETTINGS aggregate_functions_skip_variant_nulls = 0;
