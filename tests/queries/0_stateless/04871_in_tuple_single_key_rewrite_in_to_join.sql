-- The `rewrite_in_to_join` rewrite must preserve regular IN semantics when the right side is a
-- single column and the left side is a top-level tuple: regular IN compares the whole left tuple
-- against that column as a single set key, and `Set::execute` accurately casts the left value to
-- the right column type before probing. The `equals` predicate built by the rewrite compares
-- element-wise over a common supertype instead, so the rewrite is skipped for this shape and the
-- regular IN handling keeps its semantics. Regression test for the review finding in PR #97540.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET rewrite_in_to_join = 1;

-- Regular IN throws when the one-key cast of the left tuple to the right column type fails
-- (`256` does not fit into `Int8`). The rewritten path must throw the same error, not return 0.
SELECT count() FROM numbers(1) WHERE (toUInt16(256), number) IN (SELECT CAST((0, 0), 'Tuple(Int8, UInt64)')); -- { serverError CANNOT_CONVERT_TYPE }

-- NOT IN takes the same code path.
SELECT count() FROM numbers(1) WHERE (toUInt16(256), number) NOT IN (SELECT CAST((0, 0), 'Tuple(Int8, UInt64)')); -- { serverError CANNOT_CONVERT_TYPE }

-- The same one-key comparison with values that do fit keeps returning regular IN results.
SELECT (1, number) IN (SELECT CAST((1, 0), 'Tuple(UInt8, UInt64)')) FROM numbers(2);

-- A tuple wrapped into `Nullable` is kept as a single key by regular IN as well (`FunctionIn`
-- unpacks only a raw top-level tuple), so the rewrite must be skipped for that shape too.
SET enable_nullable_tuple_type = 1;
SELECT count() FROM numbers(1) WHERE CAST((toUInt16(256), number), 'Nullable(Tuple(UInt16, UInt64))') IN (SELECT CAST((0, 0), 'Tuple(Int8, UInt64)')); -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM numbers(1) WHERE CAST((toUInt16(256), number), 'Nullable(Tuple(UInt16, UInt64))') NOT IN (SELECT CAST((0, 0), 'Tuple(Int8, UInt64)')); -- { serverError CANNOT_CONVERT_TYPE }
-- A wrapped tuple key that the single right `Tuple` column can hold is a valid one-key comparison:
-- the analyzer must not reject it, and the rewrite must return the same result as regular `IN`.
SELECT CAST((1, number), 'Nullable(Tuple(UInt8, UInt64))') IN (SELECT CAST((1, 0), 'Tuple(UInt8, UInt64)')) FROM numbers(2);

-- Types with dynamic structure are rejected by `IN` itself, before any set-key casting: the
-- analyzer-time column-count validation skips them, so the regular `IN` error is preserved and
-- the rewrite must report the very same error.
SELECT count() FROM numbers(1) WHERE CAST((toUInt16(256), number), 'Dynamic') IN (SELECT CAST((0, 0), 'Tuple(Int8, UInt64)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM numbers(1) WHERE CAST('{"a":1}', 'JSON') IN (SELECT CAST('{"a":1}', 'JSON')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM numbers(1) WHERE CAST((toUInt16(256), number), 'Variant(Tuple(UInt16, UInt64), UInt8)') IN (SELECT CAST((0, 0), 'Tuple(Int8, UInt64)')); -- { serverError TYPE_MISMATCH }

-- A multi-column right side of the same arity is unpacked element-wise by regular IN, so the
-- rewrite still applies there and must keep working.
SELECT number, (number, number + 1) IN (SELECT number, number + 1 FROM numbers(3)) FROM numbers(5) ORDER BY number;

-- The plain single-column rewrite (a scalar left side) must keep working as well.
SELECT number, number IN (SELECT number FROM numbers(3)) FROM numbers(5) ORDER BY number;

-- A scalar whose type differs from the one-key subquery column needs the same accurate set-key
-- cast as a tuple. In particular, `Set::execute` parses `String` '01' as `UInt8` 1; rewriting to
-- `equals(String, UInt8)` would instead fail with `NO_COMMON_TYPE`.
SELECT count() FROM numbers(1) WHERE concat('0', toString(number + 1)) IN (SELECT toUInt8(1));

-- Equal `Nullable` scalar types need the same treatment: with `transform_null_in = 0`, regular
-- `IN` and `NOT IN` return `NULL` for a NULL key, while `equals` in the EXISTS rewrite would make
-- `NOT IN` true. Both predicates must filter the row out.
SET transform_null_in = 0;
SELECT count() FROM numbers(1) WHERE materialize(CAST(NULL, 'Nullable(UInt8)')) IN (SELECT CAST(NULL, 'Nullable(UInt8)'));
SELECT count() FROM numbers(1) WHERE materialize(CAST(NULL, 'Nullable(UInt8)')) NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)'));

-- The shapes above stay on the regular `IN` path, so they perform no correlated rewrite and must
-- not require `allow_experimental_correlated_subqueries`: enabling `rewrite_in_to_join` alone must
-- never change which queries are accepted.
SET allow_experimental_correlated_subqueries = 0;
SELECT count() FROM numbers(1) WHERE concat('0', toString(number + 1)) IN (SELECT toUInt8(1));
SELECT count() FROM numbers(1) WHERE (1, number) IN (SELECT CAST((1, 0), 'Tuple(UInt8, UInt64)'));
-- A shape that is actually rewritten still requires the setting.
SELECT count() FROM numbers(1) WHERE number IN (SELECT number FROM numbers(3)); -- { serverError SUPPORT_IS_DISABLED }
