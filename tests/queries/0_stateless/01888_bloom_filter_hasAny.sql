CREATE TABLE bftest (
    k Int64,
    x Array(Int64),
    index ix1(x) TYPE bloom_filter GRANULARITY 3
)
Engine=MergeTree
ORDER BY k;

INSERT INTO bftest SELECT number, arrayMap(i->rand64()%565656, range(10)) FROM numbers(1000);

SET force_data_skipping_indices='ix1';
SELECT count() FROM bftest WHERE has (x, 42) or has(x, -42) FORMAT Null;
SELECT count() FROM bftest WHERE hasAny(x, [42,-42]) FORMAT Null;
SELECT count() FROM bftest WHERE hasAny(x, []) FORMAT Null;
SELECT count() FROM bftest WHERE hasAny(x, [1]) FORMAT Null;

-- can't use bloom_filter with `hasAny` on non-constant arguments (just like `has`)
SELECT count() FROM bftest WHERE hasAny(x, materialize([1,2,3])) FORMAT Null; -- { serverError 277 }

SET force_data_skipping_indices='';
SELECT count() FROM bftest WHERE hasAny(x, materialize([1,2,3])) FORMAT Null;

-- NULLs are not Ok
SELECT count() FROM bftest WHERE hasAny(x, [NULL,-42]) FORMAT Null; -- { serverError 43 }
SELECT count() FROM bftest WHERE hasAny(x, [0,NULL]) FORMAT Null; -- { serverError 43 }

-- non-compatible types
SELECT count() FROM bftest WHERE hasAny(x, [[123], -42]) FORMAT Null; -- { serverError 386 }
SELECT count() FROM bftest WHERE hasAny(x, [toDecimal32(123, 3), 2]) FORMAT Null; -- { serverError 53 }

-- Bug discovered by AST fuzzier (fixed, shouldn't crash).
SELECT 1 FROM bftest WHERE has(x, -0.) OR 0. FORMAT Null;
SELECT count() FROM bftest WHERE hasAny(x, [0, 1]) OR 0. FORMAT Null;
