-- `rewrite_in_to_join` must not change what `IN` means. When the right side is a single column and
-- the left side is a top-level tuple, `IN` compares the whole left value against that column as one
-- set key, casting it to the column type first. The join keys on that same value, and the rewrite is
-- turned down wherever that cast could throw. Regression test for the review finding in PR #97540.

SET allow_correlated_subqueries = 1;
SET rewrite_in_to_join = 1;

-- An unrepresentable tuple field cannot match a set key. Since `256` does not fit into `Int8`, `IN`
-- returns 0 even with `rewrite_in_to_join` enabled.
SELECT count() FROM numbers(1) WHERE (toUInt16(256), number) IN (SELECT CAST((0, 0), 'Tuple(Int8, UInt64)'));

-- `NOT IN` returns 1 for the same non-matching key.
SELECT count() FROM numbers(1) WHERE (toUInt16(256), number) NOT IN (SELECT CAST((0, 0), 'Tuple(Int8, UInt64)'));

-- The same one-key comparison with values that do fit keeps returning regular IN results.
SELECT (1, number) IN (SELECT CAST((1, 0), 'Tuple(UInt8, UInt64)')) FROM numbers(2) ORDER BY number;

-- A tuple wrapped into `Nullable` is kept as a single key by regular IN as well (`FunctionIn`
-- unpacks only a raw top-level tuple), so the rewrite must be skipped for that shape too.
SET enable_nullable_tuple_type = 1;
SELECT count() FROM numbers(1) WHERE CAST((toUInt16(256), number), 'Nullable(Tuple(UInt16, UInt64))') IN (SELECT CAST((0, 0), 'Tuple(Int8, UInt64)'));
SELECT count() FROM numbers(1) WHERE CAST((toUInt16(256), number), 'Nullable(Tuple(UInt16, UInt64))') NOT IN (SELECT CAST((0, 0), 'Tuple(Int8, UInt64)'));
-- A wrapped tuple key that the single right `Tuple` column can hold is a valid one-key comparison:
-- the analyzer must not reject it, and the rewrite must return the same result as regular `IN`.
SELECT CAST((1, number), 'Nullable(Tuple(UInt8, UInt64))') IN (SELECT CAST((1, 0), 'Tuple(UInt8, UInt64)')) FROM numbers(2) ORDER BY number;

-- Types with dynamic structure are rejected by `IN` itself, before any set-key casting: the
-- analyzer-time column-count validation skips them, so the regular `IN` error is preserved and
-- the rewrite must report the very same error.
SELECT count() FROM numbers(1) WHERE CAST((toUInt16(256), number), 'Dynamic') IN (SELECT CAST((0, 0), 'Tuple(Int8, UInt64)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM numbers(1) WHERE CAST('{"a":1}', 'JSON') IN (SELECT CAST('{"a":1}', 'JSON')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- A subquery with several columns gets the same error, also for a left tuple wider than the subquery and
-- for a `Dynamic` value that holds a tuple.
SELECT count() FROM numbers(1) WHERE (number, number, CAST(number, 'Dynamic')) IN (SELECT number, number FROM numbers(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM numbers(1) WHERE CAST((number, number), 'Dynamic') IN (SELECT number, number FROM numbers(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- A plain `Variant` keeps a tuple value as one key and casts it like a plain tuple. `256` does not fit into
-- `Int8`, so this key does not match either.
SELECT count() FROM numbers(1) WHERE CAST((toUInt16(256), number), 'Variant(Tuple(UInt16, UInt64), UInt8)') IN (SELECT CAST((0, 0), 'Tuple(Int8, UInt64)'));

-- A multi-column right side of the same arity is unpacked element-wise by regular IN, so the
-- rewrite still applies there and must keep working.
SELECT number, (number, number + 1) IN (SELECT number, number + 1 FROM numbers(3)) FROM numbers(5) ORDER BY number;

-- The plain single-column rewrite (a scalar left side) must keep working as well.
SELECT number, number IN (SELECT number FROM numbers(3)) FROM numbers(5) ORDER BY number;

-- A scalar whose type differs from the one-key subquery column needs the same accurate set-key
-- cast as a tuple: `Set::execute` parses `String` '01' as `UInt8` 1, and the join has to key on the
-- same cast rather than compare the two types directly.
SELECT count() FROM numbers(1) WHERE concat('0', toString(number + 1)) IN (SELECT toUInt8(1));

-- Equal `Nullable` scalar types need the same treatment: with `transform_null_in = 0`, regular
-- `IN` and `NOT IN` return `NULL` for a NULL key, and the join has to mask its marker to `NULL` for
-- one too, or `NOT IN` would come out true. Both predicates must filter the row out.
SET transform_null_in = 0;
SELECT count() FROM numbers(1) WHERE materialize(CAST(NULL, 'Nullable(UInt8)')) IN (SELECT CAST(NULL, 'Nullable(UInt8)'));
SELECT count() FROM numbers(1) WHERE materialize(CAST(NULL, 'Nullable(UInt8)')) NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)'));

-- A subquery with several columns makes each element of the left tuple a key of its own, which `Set::execute`
-- converts to the type of its column. `tag` has the type of its column, so only the timestamp needs that
-- conversion. A timestamp that `Float64` cannot hold exactly matches nothing, whether the rewrite is enabled
-- or not.
SELECT (t, tag) IN (SELECT toFloat64(1767225600), 1), (t, tag) NOT IN (SELECT toFloat64(1767225600), 1)
FROM (SELECT fromUnixTimestamp64Nano(1767225600000000001, 'UTC') AS t, toUInt8(number + 1) AS tag FROM numbers(1))
SETTINGS rewrite_in_to_join = 0;
SELECT (t, tag) IN (SELECT toFloat64(1767225600), 1), (t, tag) NOT IN (SELECT toFloat64(1767225600), 1)
FROM (SELECT fromUnixTimestamp64Nano(1767225600000000001, 'UTC') AS t, toUInt8(number + 1) AS tag FROM numbers(1))
SETTINGS rewrite_in_to_join = 1;

-- Elements of the same type as their columns: a tuple element keeps its NULL fields as values, and a
-- `Variant` element keeps its NULL, so both match, while `equals` returns NULL for them. A NULL element of a
-- `Nullable` type matches nothing, as in `equals`, and `IN` of the plain tuple returns 0 rather than NULL, so
-- the rewrite applies to it.
SELECT
    (t, 1) IN (SELECT CAST((NULL, 1), 'Tuple(Nullable(UInt8), UInt8)'), 1),
    (v, 1) IN (SELECT CAST(NULL, 'Variant(UInt8, String)'), 1),
    (n, 1) IN (SELECT CAST(NULL, 'Nullable(UInt8)'), 1),
    (n, 1) NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)'), 1)
FROM
(
    SELECT
        materialize(CAST((NULL, 1), 'Tuple(Nullable(UInt8), UInt8)')) AS t,
        materialize(CAST(NULL, 'Variant(UInt8, String)')) AS v,
        materialize(CAST(NULL, 'Nullable(UInt8)')) AS n
)
SETTINGS rewrite_in_to_join = 0;
SELECT
    (t, 1) IN (SELECT CAST((NULL, 1), 'Tuple(Nullable(UInt8), UInt8)'), 1),
    (v, 1) IN (SELECT CAST(NULL, 'Variant(UInt8, String)'), 1),
    (n, 1) IN (SELECT CAST(NULL, 'Nullable(UInt8)'), 1),
    (n, 1) NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)'), 1)
FROM
(
    SELECT
        materialize(CAST((NULL, 1), 'Tuple(Nullable(UInt8), UInt8)')) AS t,
        materialize(CAST(NULL, 'Variant(UInt8, String)')) AS v,
        materialize(CAST(NULL, 'Nullable(UInt8)')) AS n
)
SETTINGS rewrite_in_to_join = 1;

-- Keys of the same type as their columns that the set still compares unlike `equals`: it converts a key of a
-- `Bool` column, so the byte 2 is `true`, and it compares floating-point values by their bits, so a NaN matches
-- itself. Negative zeros are replaced by positive zeros in the keys, so `-0.0` matches `0.0` as in `equals`, also
-- inside an `Array`.
SELECT (b, 1) IN (SELECT true, 1), (b, 1) NOT IN (SELECT true, 1), f IN (SELECT nan), [z] IN (SELECT [0.])
FROM (SELECT materialize(toUInt8(2)) AS b, materialize(nan) AS f, materialize(-0.) AS z)
SETTINGS rewrite_in_to_join = 0;
SELECT (b, 1) IN (SELECT true, 1), (b, 1) NOT IN (SELECT true, 1), f IN (SELECT nan), [z] IN (SELECT [0.])
FROM (SELECT materialize(toUInt8(2)) AS b, materialize(nan) AS f, materialize(-0.) AS z)
SETTINGS rewrite_in_to_join = 1;
