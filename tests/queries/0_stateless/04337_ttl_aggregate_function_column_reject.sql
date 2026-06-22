-- Verify that CREATE TABLE rejects TTL expressions referencing AggregateFunction columns at DDL time.

-- Table-level TTL: toDateTime cannot accept AggregateFunction state
CREATE TABLE test_ttl_agg
(
    key1 String,
    key2 String,
    ts AggregateFunction(max, DateTime64(3))
)
ENGINE = MergeTree()
ORDER BY (key1, key2)
TTL toDateTime(ts) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

-- Column-level TTL: same issue
CREATE TABLE test_ttl_agg_col
(
    key1 String,
    key2 String,
    ts AggregateFunction(max, DateTime64(3)) TTL toDateTime(ts) + INTERVAL 1 DAY
)
ENGINE = MergeTree()
ORDER BY (key1, key2); -- { serverError BAD_TTL_EXPRESSION }

-- TTL DELETE WHERE: toDateTime on AggregateFunction in WHERE clause
CREATE TABLE test_ttl_agg_where
(
    key1 String,
    key2 String,
    d DateTime,
    ts AggregateFunction(max, DateTime64(3))
)
ENGINE = MergeTree()
ORDER BY (key1, key2)
TTL d + INTERVAL 1 DAY DELETE WHERE toDateTime(ts) > toDateTime(0); -- { serverError BAD_TTL_EXPRESSION }

-- AggregateFunction passed directly to arithmetic (plus)
CREATE TABLE test_ttl_agg_plus
(
    key1 String,
    ts AggregateFunction(max, DateTime64(3))
)
ENGINE = MergeTree()
ORDER BY key1
TTL ts + INTERVAL 1 DAY; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Nullable-wrapped conversion: CAST to Nullable(DateTime) should still be caught
CREATE TABLE test_ttl_agg_nullable
(
    key1 String,
    ts AggregateFunction(max, DateTime64(3))
)
ENGINE = MergeTree()
ORDER BY key1
TTL assumeNotNull(CAST(ts, 'Nullable(DateTime)')) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

-- Non-date intermediate conversion: toUInt32(aggfunc) fails at execution time too
CREATE TABLE test_ttl_agg_touint
(
    ts AggregateFunction(max, UInt32)
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(toUInt32(ts)) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

-- Nested state inside a Tuple: the AggregateFunction is not the top-level type, so it must be
-- found via the recursive type walk 
CREATE TABLE test_ttl_agg_tuple
(
    key1 String,
    ts Tuple(a UInt32, b AggregateFunction(max, DateTime64(3)))
)
ENGINE = MergeTree()
ORDER BY key1
TTL toDateTime(ts.2) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

-- Nested state inside an Array.
CREATE TABLE test_ttl_agg_array
(
    key1 String,
    ts Array(AggregateFunction(max, DateTime64(3)))
)
ENGINE = MergeTree()
ORDER BY key1
TTL toDateTime(ts[1]) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

-- Nested state inside a Map.
CREATE TABLE test_ttl_agg_map
(
    key1 String,
    ts Map(String, AggregateFunction(max, DateTime64(3)))
)
ENGINE = MergeTree()
ORDER BY key1
TTL toDateTime(ts['a']) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

-- Nested state used only in DELETE WHERE: must be caught on the WHERE path too.
CREATE TABLE test_ttl_agg_tuple_where
(
    key1 String,
    d DateTime,
    ts Tuple(a UInt32, b AggregateFunction(max, DateTime64(3)))
)
ENGINE = MergeTree()
ORDER BY key1
TTL d + INTERVAL 1 DAY DELETE WHERE toDateTime(ts.2) > toDateTime(0); -- { serverError BAD_TTL_EXPRESSION }

-- Valid: a nested AggregateFunction state that is not referenced by the TTL must be accepted.
CREATE TABLE test_ttl_agg_tuple_not_referenced
(
    key1 String,
    d DateTime,
    ts Tuple(a UInt32, b AggregateFunction(max, DateTime64(3)))
)
ENGINE = MergeTree()
ORDER BY key1
TTL d + INTERVAL 1 DAY;

DROP TABLE test_ttl_agg_tuple_not_referenced;

-- Non-type errors raised while dry-running the AggregateFunction validation must be propagated to
-- the user.Only ILLEGAL_TYPE_OF_ARGUMENT is translated to BAD_TTL_EXPRESSION; everything else is rethrown.
CREATE TABLE test_ttl_agg_divzero
(
    ts AggregateFunction(sum, UInt32)
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(intDiv(toUInt32(100), finalizeAggregation(ts))) + INTERVAL 1 DAY; -- { serverError ILLEGAL_DIVISION }

-- Short-circuit branch: an unsupported AggregateFunction consumer hidden in a not-taken if/multiIf
-- branch must still be rejected. With short-circuit evaluation the synthetic validation row (cond = 0)
-- would skip the toDateTime(ts) branch, so validation must run with short-circuit disabled.
CREATE TABLE test_ttl_agg_if_branch
(
    cond UInt8,
    ts AggregateFunction(max, DateTime64(3))
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL if(cond, toDateTime(ts), toDateTime(finalizeAggregation(ts))) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

-- Valid: finalizeAggregation can operate on AggregateFunction states
CREATE TABLE test_ttl_agg_finalize
(
    key1 String,
    key2 String,
    ts AggregateFunction(max, DateTime64(3))
)
ENGINE = MergeTree()
ORDER BY (key1, key2)
TTL toDateTime(finalizeAggregation(ts)) + INTERVAL 1 DAY;

DROP TABLE test_ttl_agg_finalize;

-- Valid: state-aware functions like bitmapCardinality properly accept AggregateFunction
CREATE TABLE test_ttl_agg_bitmap
(
    k UInt64,
    bm AggregateFunction(groupBitmap, UInt64)
)
ENGINE = MergeTree()
ORDER BY k
TTL toDateTime(bitmapCardinality(bm)) + INTERVAL 1 DAY;

DROP TABLE test_ttl_agg_bitmap;

-- Valid: expressions with potential division by zero should NOT be rejected at DDL time
CREATE TABLE test_ttl_intdiv
(
    ts UInt32,
    denom UInt32 DEFAULT 1
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(intDiv(ts, denom)) + INTERVAL 1 DAY;

DROP TABLE test_ttl_intdiv;

-- Valid: AggregateFunction column exists but is not referenced in TTL
CREATE TABLE test_ttl_agg_not_referenced
(
    key1 String,
    d DateTime,
    ts AggregateFunction(max, DateTime64(3))
)
ENGINE = MergeTree()
ORDER BY key1
TTL d + INTERVAL 1 DAY;

DROP TABLE test_ttl_agg_not_referenced;

-- Valid: normal DateTime column in TTL (sanity check)
CREATE TABLE test_ttl_normal
(
    key1 String,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key1
TTL d + INTERVAL 1 DAY;

DROP TABLE test_ttl_normal;
