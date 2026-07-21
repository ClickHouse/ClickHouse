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
-- found via the recursive type walk.
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

-- Valid: a data-dependent error of a function that does not itself consume the AggregateFunction state
-- must NOT fail validation.
CREATE TABLE test_ttl_agg_divzero
(
    ts AggregateFunction(sum, UInt32)
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(intDiv(toUInt32(100), finalizeAggregation(ts))) + INTERVAL 1 DAY;

DROP TABLE test_ttl_agg_divzero;

-- Short-circuit branch: an unsupported AggregateFunction consumer hidden in a not-taken if/multiIf
-- branch must still be rejected.
CREATE TABLE test_ttl_agg_if_branch
(
    cond UInt8,
    ts AggregateFunction(max, DateTime64(3))
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL if(cond, toDateTime(ts), toDateTime(finalizeAggregation(ts))) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

-- Lambda body: an unsupported AggregateFunction consumer inside a higher-order function's lambda must
-- be rejected. Validation recurses into the lambda DAG instead of executing the outer arrayMap over the
-- empty default array (which would never reach the lambda body).
CREATE TABLE test_ttl_agg_lambda
(
    ts Array(AggregateFunction(max, DateTime64(3)))
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL arrayMap(x -> toDateTime(x), ts)[1] + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

-- Valid: a state-aware consumer inside a lambda body must be accepted.
CREATE TABLE test_ttl_agg_lambda_finalize
(
    ts Array(AggregateFunction(max, DateTime64(3)))
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL arrayMap(x -> toDateTime(finalizeAggregation(x)), ts)[1] + INTERVAL 1 DAY;

DROP TABLE test_ttl_agg_lambda_finalize;

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

-- GROUP BY SET: an unsupported AggregateFunction consumer inside a SET aggregate argument must be
-- rejected.
CREATE TABLE test_ttl_agg_group_by_set
(
    key UInt64,
    d DateTime,
    ts AggregateFunction(max, DateTime64(3)),
    out DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY GROUP BY key SET out = max(toDateTime(ts)); -- { serverError BAD_TTL_EXPRESSION }

-- Valid: a state-aware consumer inside a SET aggregate argument must be accepted.
CREATE TABLE test_ttl_agg_group_by_set_finalize
(
    key UInt64,
    d DateTime,
    ts AggregateFunction(max, DateTime64(3)),
    out DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY GROUP BY key SET out = max(toDateTime(finalizeAggregation(ts)));

DROP TABLE test_ttl_agg_group_by_set_finalize;

-- GROUP BY SET: an aggregate that returns the AggregateFunction state itself (e.g. `any(ts)`) and is then
-- implicitly cast to an incompatible target column type must be rejected. The aggregate argument is just
-- `ts`, so this is caught by validating the post-aggregation (casted) SET expression, not the argument.
CREATE TABLE test_ttl_agg_group_by_set_cast
(
    key UInt64,
    d DateTime,
    ts AggregateFunction(max, DateTime64(3)),
    out DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY GROUP BY key SET out = any(ts); -- { serverError BAD_TTL_EXPRESSION }

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

-- Variant column carrying an AggregateFunction alternative: the all-NULL default probe column would let
-- the Variant function adaptor short-circuit, so this used to pass CREATE TABLE and only fail at insert.
-- Table-level TTL: toDateTime cannot consume the AggregateFunction alternative of the Variant.
CREATE TABLE test_ttl_agg_variant
(
    key UInt64,
    v Variant(AggregateFunction(max, DateTime64(3)), String),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL toDateTime(v) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

-- Column-level TTL on the Variant: same issue.
CREATE TABLE test_ttl_agg_variant_col
(
    key UInt64,
    v Variant(AggregateFunction(max, DateTime64(3)), String) TTL toDateTime(v) + INTERVAL 1 DAY
)
ENGINE = MergeTree()
ORDER BY key; -- { serverError BAD_TTL_EXPRESSION }

-- TTL DELETE WHERE using the Variant alternative.
CREATE TABLE test_ttl_agg_variant_where
(
    key UInt64,
    d DateTime,
    v Variant(AggregateFunction(max, DateTime64(3)), String)
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE toDateTime(v) > toDateTime(0); -- { serverError BAD_TTL_EXPRESSION }

-- Every AggregateFunction alternative is probed, so a Variant of two different states is also rejected.
CREATE TABLE test_ttl_agg_variant_two
(
    key UInt64,
    v Variant(AggregateFunction(max, DateTime64(3)), AggregateFunction(sum, UInt64)),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL toDateTime(v) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

-- Valid: a state-aware consumer (`finalizeAggregation`) reaching the AggregateFunction alternative of a
-- Variant must still be accepted - only the aggregate-carrying alternative is probed, not the consumer.
CREATE TABLE test_ttl_agg_variant_finalize
(
    key UInt64,
    v Variant(AggregateFunction(max, DateTime64(3))),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL toDateTime(assumeNotNull(finalizeAggregation(v))) + INTERVAL 1 DAY;

DROP TABLE test_ttl_agg_variant_finalize;

-- Valid: a Variant with an AggregateFunction alternative that is not referenced in the TTL is accepted.
CREATE TABLE test_ttl_agg_variant_not_referenced
(
    key UInt64,
    d DateTime,
    v Variant(AggregateFunction(max, DateTime64(3)), String)
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY;

DROP TABLE test_ttl_agg_variant_not_referenced;

-- Valid: the escape hatch `allow_suspicious_ttl_expressions` still lets the rejected expression through.
SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_agg_variant_suspicious
(
    key UInt64,
    v Variant(AggregateFunction(max, DateTime64(3)), String),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL toDateTime(v) + INTERVAL 1 DAY;

DROP TABLE test_ttl_agg_variant_suspicious;

SET allow_suspicious_ttl_expressions = 0;

-- Dynamic column: the static type never mentions AggregateFunction, but any row may carry a state
-- (e.g. inserted via CAST to Dynamic), so this used to pass CREATE TABLE and only fail during TTL
-- execution. The validator probes Dynamic arguments with a synthetic state.
-- Table-level TTL: toDateTime cannot consume an AggregateFunction state stored in the Dynamic.
CREATE TABLE test_ttl_agg_dynamic
(
    key UInt64,
    dyn Dynamic,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL toDateTime(dyn) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

-- Column-level TTL on the Dynamic: same issue.
CREATE TABLE test_ttl_agg_dynamic_col
(
    key UInt64,
    dyn Dynamic TTL toDateTime(dyn) + INTERVAL 1 DAY
)
ENGINE = MergeTree()
ORDER BY key; -- { serverError BAD_TTL_EXPRESSION }

-- TTL DELETE WHERE using the Dynamic column.
CREATE TABLE test_ttl_agg_dynamic_where
(
    key UInt64,
    d DateTime,
    dyn Dynamic
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE toDateTime(dyn) > toDateTime(0); -- { serverError BAD_TTL_EXPRESSION }

-- Dynamic with no room for new variants stores every value in the shared variant; the probe goes
-- through the shared variant and the expression is still rejected.
CREATE TABLE test_ttl_agg_dynamic_shared
(
    key UInt64,
    dyn Dynamic(max_types=0),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL toDateTime(dyn) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

-- Valid: a type-agnostic consumer (`isNotNull`) can handle any stored value, including a state.
CREATE TABLE test_ttl_agg_dynamic_agnostic
(
    key UInt64,
    d DateTime,
    dyn Dynamic
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNotNull(dyn);

DROP TABLE test_ttl_agg_dynamic_agnostic;

-- Valid: a Dynamic column that is not referenced in the TTL is accepted.
CREATE TABLE test_ttl_agg_dynamic_not_referenced
(
    key UInt64,
    d DateTime,
    dyn Dynamic
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY;

DROP TABLE test_ttl_agg_dynamic_not_referenced;

-- Valid: the escape hatch `allow_suspicious_ttl_expressions` still lets the rejected expression through.
SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_agg_dynamic_suspicious
(
    key UInt64,
    dyn Dynamic,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL toDateTime(dyn) + INTERVAL 1 DAY;

DROP TABLE test_ttl_agg_dynamic_suspicious;

SET allow_suspicious_ttl_expressions = 0;

-- Lowering `variant_throw_on_type_mismatch` to 0 makes the Variant function adaptor return NULL instead of
-- throwing on a type mismatch. The validation probe must still reject a suspicious TTL, because TTL merges
-- rebuild the expression under the background context (strict by default) and would otherwise break every merge.
SET variant_throw_on_type_mismatch = 0;

CREATE TABLE test_ttl_agg_variant_lenient
(
    key UInt64,
    v Variant(AggregateFunction(max, DateTime64(3)), String),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL toDateTime(v) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

SET variant_throw_on_type_mismatch = 1;

-- Same, for `dynamic_throw_on_type_mismatch`: a lenient session must not let a suspicious Dynamic TTL through.
SET dynamic_throw_on_type_mismatch = 0;

CREATE TABLE test_ttl_agg_dynamic_lenient
(
    key UInt64,
    dyn Dynamic,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL toDateTime(dyn) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

-- A type-agnostic Dynamic consumer is still accepted even under the lenient setting.
CREATE TABLE test_ttl_agg_dynamic_lenient_agnostic
(
    key UInt64,
    d DateTime,
    dyn Dynamic
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNotNull(dyn);

DROP TABLE test_ttl_agg_dynamic_lenient_agnostic;

SET dynamic_throw_on_type_mismatch = 1;

-- The conversion functions above (`toDateTime`) ignore the mismatch settings, so they alone cannot tell
-- which settings the probe runs under. Consumers that go through the `Variant`/`Dynamic` function adaptors
-- (e.g. `length`) do honor the settings: under a lenient session the adaptor would silently return NULL in
-- the probe, while the strict TTL execution paths - a default-settings INSERT computing TTLs in
-- `MergeTreeDataWriter::updateTTL`, background merges under the default `background_profile`, and table
-- loading on restart (no query context, adaptors fall back to strict) - would throw on the first row
-- carrying an AggregateFunction state. The probe therefore always runs strict, regardless of the session,
-- so a lenient session must still get such a TTL rejected.
SET dynamic_throw_on_type_mismatch = 0;

CREATE TABLE test_ttl_agg_dynamic_lenient_adaptor
(
    key UInt64,
    dyn Dynamic,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE length(dyn) > 3; -- { serverError BAD_TTL_EXPRESSION }

SET dynamic_throw_on_type_mismatch = 1;

SET variant_throw_on_type_mismatch = 0;

CREATE TABLE test_ttl_agg_variant_lenient_adaptor
(
    key UInt64,
    v Variant(AggregateFunction(max, DateTime64(3)), String),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE length(v) > 3; -- { serverError BAD_TTL_EXPRESSION }

SET variant_throw_on_type_mismatch = 1;

-- The Variant function adaptor also consults `variant_throw_on_type_mismatch` while *building* the
-- expression: when none of the alternatives is compatible with the consumer, the build itself either
-- throws (strict) or resolves the result to constant NULL (lenient). The lenient build must not slip
-- through DDL validation: the constant fold prunes the referenced column from the stored TTL column list,
-- so every later rebuild of the TTL expression fails with "Missing columns" (broken INSERTs and merges),
-- and the table cannot be re-attached on restart (loading has no query context, so the adaptor is strict
-- and throws). The validation build therefore always runs strict, and a lenient session gets the same
-- rejection a strict one does.
SET variant_throw_on_type_mismatch = 0;

CREATE TABLE test_ttl_agg_variant_lenient_build
(
    key UInt64,
    v Variant(AggregateFunction(max, UInt64)),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNull(length(v)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The escape hatch keeps the plain session behavior: with `allow_suspicious_ttl_expressions` the lenient
-- session resolves the all-incompatible consumer to NULL at build time and the CREATE is accepted.
SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_agg_variant_lenient_build_suspicious
(
    key UInt64,
    v Variant(AggregateFunction(max, UInt64)),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNull(length(v));

DROP TABLE test_ttl_agg_variant_lenient_build_suspicious;

SET allow_suspicious_ttl_expressions = 0;
SET variant_throw_on_type_mismatch = 1;

-- A consumer over *several* Variant/Dynamic carriers must be probed with all of them materialized
-- simultaneously: substituting one at a time leaves the other side at its all-NULL default, the adaptor
-- short-circuits to NULL, and the bad joint combination (e.g. state + state) is never built or executed -
-- so the CREATE passed while a row with states on both sides still threw during TTL execution.
CREATE TABLE test_ttl_agg_two_dynamic
(
    key UInt64,
    d1 Dynamic,
    d2 Dynamic,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE d1 = d2; -- { serverError BAD_TTL_EXPRESSION }

CREATE TABLE test_ttl_agg_two_variant
(
    key UInt64,
    v1 Variant(AggregateFunction(max, UInt64), UInt64),
    v2 Variant(AggregateFunction(max, UInt64), UInt64),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE v1 = v2; -- { serverError BAD_TTL_EXPRESSION }

-- A joint consumer that handles every alternative combination is still accepted.
CREATE TABLE test_ttl_agg_two_carriers_agnostic
(
    key UInt64,
    d1 Dynamic,
    v1 Variant(AggregateFunction(max, UInt64), UInt64),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE concat(toString(isNotNull(d1)), toString(isNotNull(v1))) = '11';

DROP TABLE test_ttl_agg_two_carriers_agnostic;

-- The escape hatch also covers the joint case.
SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_agg_two_dynamic_suspicious
(
    key UInt64,
    d1 Dynamic,
    d2 Dynamic,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE d1 = d2;

DROP TABLE test_ttl_agg_two_dynamic_suspicious;

SET allow_suspicious_ttl_expressions = 0;

-- A state-aware consumer over a mixed Variant is rejected even when it can consume the
-- AggregateFunction alternative, because it still throws on a sibling alternative a later row may store:
-- `finalizeAggregation` accepts the AggregateFunction branch but throws ILLEGAL_TYPE_OF_ARGUMENT on the UInt32 branch.
CREATE TABLE test_ttl_agg_mixed_variant_finalize
(
    key UInt64,
    v Variant(AggregateFunction(max, UInt32), UInt32),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNotNull(finalizeAggregation(v)); -- { serverError BAD_TTL_EXPRESSION }

-- A Variant every alternative of which the consumer can handle is still accepted.
CREATE TABLE test_ttl_agg_all_state_alternatives
(
    key UInt64,
    v Variant(AggregateFunction(max, UInt32), AggregateFunction(min, UInt32)),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNotNull(finalizeAggregation(v));

DROP TABLE test_ttl_agg_all_state_alternatives;

-- A type-agnostic consumer over the same mixed Variant is still accepted.
CREATE TABLE test_ttl_agg_mixed_variant_agnostic
(
    key UInt64,
    v Variant(AggregateFunction(max, UInt32), UInt32),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNotNull(v);

DROP TABLE test_ttl_agg_mixed_variant_agnostic;

-- The escape hatch also covers the mixed-Variant case.
SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_agg_mixed_variant_suspicious
(
    key UInt64,
    v Variant(AggregateFunction(max, UInt32), UInt32),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNotNull(finalizeAggregation(v));

DROP TABLE test_ttl_agg_mixed_variant_suspicious;

SET allow_suspicious_ttl_expressions = 0;
