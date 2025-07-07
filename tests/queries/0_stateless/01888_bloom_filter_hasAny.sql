CREATE TABLE bftest (
    k Int64,
    y Array(Int64) DEFAULT x,
    x Array(Int64),
    index ix1(x) TYPE bloom_filter GRANULARITY 3
)
Engine=MergeTree
ORDER BY k;

INSERT INTO bftest (k, x) SELECT number, arrayMap(i->rand64()%565656, range(10)) FROM numbers(1000);

-- index is not used, but query should still work
SELECT count() FROM bftest WHERE hasAny(x, materialize([1,2,3])) FORMAT Null;

-- verify the expression in WHERE works on non-index col the same way as on index cols
SELECT count() FROM bftest WHERE hasAny(y, [NULL,-42]) FORMAT Null;
SELECT count() FROM bftest WHERE hasAny(y, [0,NULL]) FORMAT Null;
SELECT count() FROM bftest WHERE hasAny(y, [[123], -42]) FORMAT Null; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM bftest WHERE hasAny(y, [toDecimal32(123, 3), 2]) FORMAT Null; -- different, doesn't fail

SET force_data_skipping_indices='ix1';
SELECT count() FROM bftest WHERE has (x, 42) or has(x, -42) FORMAT Null;
SELECT count() FROM bftest WHERE hasAny(x, [42,-42]) FORMAT Null;
SELECT count() FROM bftest WHERE hasAny(x, []) FORMAT Null;
SELECT count() FROM bftest WHERE hasAny(x, [1]) FORMAT Null;

-- can't use bloom_filter with `hasAny` on non-constant arguments (just like `has`)
SELECT count() FROM bftest WHERE hasAny(x, [1,2,k]) FORMAT Null; -- { serverError INDEX_NOT_USED }

-- NULLs are not Ok
SELECT count() FROM bftest WHERE hasAny(x, [NULL,-42]) FORMAT Null; -- { serverError INDEX_NOT_USED }
SELECT count() FROM bftest WHERE hasAny(x, [0,NULL]) FORMAT Null; -- { serverError INDEX_NOT_USED }

-- non-compatible types
SELECT count() FROM bftest WHERE hasAny(x, [[123], -42]) FORMAT Null; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM bftest WHERE hasAny(x, [toDecimal32(123, 3), 2]) FORMAT Null; -- { serverError INDEX_NOT_USED }

-- Bug discovered by AST fuzzier (fixed, shouldn't crash).
SELECT 1 FROM bftest WHERE has(x, -0.) OR 0. FORMAT Null;
SELECT count() FROM bftest WHERE hasAny(x, [0, 1]) OR 0. FORMAT Null;
