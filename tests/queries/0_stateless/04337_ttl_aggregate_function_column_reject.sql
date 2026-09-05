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

-- A state-aware consumer over a Dynamic is rejected even when it can consume an AggregateFunction state,
-- because a Dynamic can store any type and the consumer still throws on other legal payloads:
-- `finalizeAggregation` accepts the synthetic state but throws ILLEGAL_TYPE_OF_ARGUMENT on a UInt64 / String row.
CREATE TABLE test_ttl_agg_dynamic_finalize
(
    key UInt64,
    dyn Dynamic,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNotNull(finalizeAggregation(dyn)); -- { serverError BAD_TTL_EXPRESSION }

-- A type-agnostic Dynamic consumer that handles every representative payload is still accepted.
CREATE TABLE test_ttl_agg_dynamic_finalize_agnostic
(
    key UInt64,
    d DateTime,
    dyn Dynamic
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNotNull(dyn) AND dynamicType(dyn) != 'UInt64';

DROP TABLE test_ttl_agg_dynamic_finalize_agnostic;

-- The escape hatch also covers the state-aware Dynamic case.
SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_agg_dynamic_finalize_suspicious
(
    key UInt64,
    dyn Dynamic,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNotNull(finalizeAggregation(dyn));

DROP TABLE test_ttl_agg_dynamic_finalize_suspicious;

SET allow_suspicious_ttl_expressions = 0;

-- A Variant/Dynamic carrier nested inside a container argument (Array/Tuple/Map) must be probed too:
-- the container's default value is empty, so a consumer that processes the elements (e.g. the `equals`
-- built inside `arrayRemove`) never sees a payload during a default-value probe, yet still rebuilds per
-- stored payload during TTL execution and throws on the first element carrying an AggregateFunction state.
CREATE TABLE test_ttl_agg_array_dynamic
(
    key UInt64,
    arr Array(Dynamic),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE notEmpty(arrayRemove(arr, 0)); -- { serverError BAD_TTL_EXPRESSION }

-- The same through a Map value.
CREATE TABLE test_ttl_agg_map_dynamic
(
    key UInt64,
    m Map(String, Dynamic),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE notEmpty(arrayRemove(mapValues(m), 0)); -- { serverError BAD_TTL_EXPRESSION }

-- The same through a Tuple element.
CREATE TABLE test_ttl_agg_tuple_dynamic
(
    key UInt64,
    tup Tuple(UInt32, Dynamic),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE notEmpty(arrayRemove([tup], (0, 0)::Tuple(UInt32, Dynamic))); -- { serverError BAD_TTL_EXPRESSION }

-- A Variant with an AggregateFunction alternative nested inside an Array is probed per alternative too.
CREATE TABLE test_ttl_agg_array_variant
(
    key UInt64,
    arr Array(Variant(AggregateFunction(max, UInt32), UInt32)),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE notEmpty(arrayRemove(arr, 0)); -- { serverError BAD_TTL_EXPRESSION }

-- Valid: a consumer that does not touch the elements is accepted.
CREATE TABLE test_ttl_agg_array_dynamic_agnostic
(
    key UInt64,
    arr Array(Dynamic),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE length(arr) > 3;

DROP TABLE test_ttl_agg_array_dynamic_agnostic;

-- Valid: a nested carrier that is not referenced in the TTL is accepted.
CREATE TABLE test_ttl_agg_array_dynamic_not_referenced
(
    key UInt64,
    arr Array(Dynamic),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY;

DROP TABLE test_ttl_agg_array_dynamic_not_referenced;

-- The escape hatch also covers the nested-carrier case.
SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_agg_array_dynamic_suspicious
(
    key UInt64,
    arr Array(Dynamic),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE notEmpty(arrayRemove(arr, 0));

DROP TABLE test_ttl_agg_array_dynamic_suspicious;

SET allow_suspicious_ttl_expressions = 0;

-- A carrier hidden under a Nullable wrapper (Nullable(Tuple(..., Dynamic))) must be probed with a
-- non-NULL row too: the default Nullable row is NULL, so the consumer would otherwise never see the
-- nested payload at DDL time, yet still throw during TTL execution once a non-NULL row stores a state.
SET enable_nullable_tuple_type = 1;

CREATE TABLE test_ttl_agg_nullable_tuple_dynamic
(
    key UInt64,
    tup Nullable(Tuple(UInt32, Dynamic)),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE tup = tup; -- { serverError BAD_TTL_EXPRESSION }

-- Valid: a type-agnostic consumer over the Nullable wrapper is accepted.
CREATE TABLE test_ttl_agg_nullable_tuple_agnostic
(
    key UInt64,
    tup Nullable(Tuple(UInt32, Dynamic)),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNotNull(tup);

DROP TABLE test_ttl_agg_nullable_tuple_agnostic;

-- The escape hatch also covers the Nullable-wrapped carrier.
SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_agg_nullable_tuple_suspicious
(
    key UInt64,
    tup Nullable(Tuple(UInt32, Dynamic)),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE tup = tup;

DROP TABLE test_ttl_agg_nullable_tuple_suspicious;

SET allow_suspicious_ttl_expressions = 0;
SET enable_nullable_tuple_type = 0;

-- A direct AggregateFunction state nested in a container (not through a Variant/Dynamic carrier) must be
-- probed with a non-empty row too: the default Array/Map value is empty, so an element-level consumer
-- (e.g. the `equals` built inside `arrayRemove`) would otherwise never see the state at DDL time, yet
-- still throw during TTL execution once a row stores a state.
CREATE TABLE test_ttl_agg_array_state
(
    key UInt64,
    arr Array(AggregateFunction(max, UInt64)),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE notEmpty(arrayRemove(arr, 0)); -- { serverError BAD_TTL_EXPRESSION }

CREATE TABLE test_ttl_agg_map_state
(
    key UInt64,
    m Map(String, AggregateFunction(max, UInt64)),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE notEmpty(arrayRemove(mapValues(m), mapValues(m)[1])); -- { serverError BAD_TTL_EXPRESSION }

-- Valid: consumers that do not look into the state elements are accepted.
CREATE TABLE test_ttl_agg_array_state_agnostic
(
    key UInt64,
    arr Array(AggregateFunction(max, UInt64)),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE length(arr) > 3;

DROP TABLE test_ttl_agg_array_state_agnostic;

-- The escape hatch also covers the container-nested state.
SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_agg_array_state_suspicious
(
    key UInt64,
    arr Array(AggregateFunction(max, UInt64)),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE notEmpty(arrayRemove(arr, 0));

DROP TABLE test_ttl_agg_array_state_suspicious;

SET allow_suspicious_ttl_expressions = 0;

-- A carrier *computed* from an AggregateFunction state has a narrower runtime domain than its static
-- type: `CAST(state, 'Dynamic')` or `CAST(state, 'Variant(AggregateFunction(max, UInt32), UInt32)')`
-- only ever produces the aggregate-state payload, so a state-aware consumer over it is valid. The probe
-- must validate it against the child's actual output, not fabricate the impossible sibling payloads.
CREATE TABLE test_ttl_agg_computed_dynamic_accept
(
    key UInt64,
    state AggregateFunction(max, UInt64),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNotNull(finalizeAggregation(CAST(state, 'Dynamic')));

DROP TABLE test_ttl_agg_computed_dynamic_accept;

CREATE TABLE test_ttl_agg_computed_variant_accept
(
    key UInt64,
    state AggregateFunction(max, UInt32),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNotNull(finalizeAggregation(CAST(state, 'Variant(AggregateFunction(max, UInt32), UInt32)')));

DROP TABLE test_ttl_agg_computed_variant_accept;

-- The computed carrier still holds the state: a consumer that cannot handle it stays rejected.
CREATE TABLE test_ttl_agg_computed_carrier_reject
(
    key UInt64,
    state AggregateFunction(max, UInt64),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL toDateTime(CAST(state, 'Dynamic')); -- { serverError BAD_TTL_EXPRESSION }

-- A typed CAST of a non-suspect column propagates its actual output domain: here the runtime payload
-- is always `UInt32` (the cast picks the matching alternative), which `finalizeAggregation` cannot
-- consume, so the TTL is rejected against the real domain.
CREATE TABLE test_ttl_agg_untainted_carrier_reject
(
    key UInt64,
    num UInt32,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNotNull(finalizeAggregation(CAST(num, 'Variant(AggregateFunction(max, UInt32), UInt32)'))); -- { serverError BAD_TTL_EXPRESSION }

-- The escape hatch also covers consumers of computed carriers.
SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_agg_computed_carrier_suspicious
(
    key UInt64,
    state AggregateFunction(max, UInt64),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL toDateTime(CAST(state, 'Dynamic'));

DROP TABLE test_ttl_agg_computed_carrier_suspicious;

SET allow_suspicious_ttl_expressions = 0;

-- A non-constant non-suspect argument can select which payload a computed carrier holds, so the probe
-- outputs (taken with synthetic defaults, here `cond = 0`) do not cover its runtime domain: with
-- `cond = 1` the `if` returns the aggregate-state branch and `toDateTime` would throw during TTL
-- execution. Such nodes fall back to the fail-closed static enumeration and the consumer is rejected.
CREATE TABLE test_ttl_agg_selected_carrier_reject
(
    cond UInt8,
    state AggregateFunction(max, UInt64),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(if(cond, CAST(state, 'Dynamic'), CAST(0, 'Dynamic'))); -- { serverError BAD_TTL_EXPRESSION }

-- A type-agnostic consumer survives the static enumeration, so the same selected carrier is accepted.
CREATE TABLE test_ttl_agg_selected_carrier_accept
(
    cond UInt8,
    state AggregateFunction(max, UInt64),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL d + INTERVAL 1 DAY DELETE WHERE isNotNull(if(cond, CAST(state, 'Dynamic'), CAST(0, 'Dynamic')));

DROP TABLE test_ttl_agg_selected_carrier_accept;

-- The escape hatch also covers payload-selecting expressions.
SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_agg_selected_carrier_suspicious
(
    cond UInt8,
    state AggregateFunction(max, UInt64),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(if(cond, CAST(state, 'Dynamic'), CAST(0, 'Dynamic')));

DROP TABLE test_ttl_agg_selected_carrier_suspicious;

SET allow_suspicious_ttl_expressions = 0;

-- A typed CAST of an ordinary column to Dynamic can only ever store the payload type derived from the
-- source type (here `UInt32`), so its consumer must not be probed with synthetic AggregateFunction
-- payloads the cast can never produce.
CREATE TABLE test_ttl_cast_plain_number_accept
(
    n UInt32,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL d + INTERVAL 1 DAY DELETE WHERE toDateTime(CAST(n, 'Dynamic')) < d;

DROP TABLE test_ttl_cast_plain_number_accept;

-- The representative source value for the cast probe is non-NULL, so a Nullable source still exercises
-- the consumer on the actual payload type instead of a NULL row that would short-circuit it.
CREATE TABLE test_ttl_cast_nullable_number_accept
(
    nn Nullable(UInt32),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL d + INTERVAL 1 DAY DELETE WHERE toDateTime(CAST(nn, 'Dynamic')) < d;

DROP TABLE test_ttl_cast_nullable_number_accept;

-- Containers are materialized with one element for the cast probe, so element-level consumers of the
-- cast result are validated against the actual element payload type (accepted: `toDateTime` of a
-- `UInt32` element works) instead of an empty default that would hide them. Note this holds for direct
-- consumers only: a lambda body (e.g. inside `arrayExists`) is validated through the captured DAG,
-- where the element is a plain `Dynamic` input, so it keeps the fail-closed static enumeration.
CREATE TABLE test_ttl_cast_array_accept
(
    arr Array(UInt32),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL d + INTERVAL 1 DAY DELETE WHERE toDateTime(arrayElement(CAST(arr, 'Array(Dynamic)'), 1)) < d;

DROP TABLE test_ttl_cast_array_accept;

-- A cast of a *string* to a carrier is not source-type-determined: `cast_string_to_variant_use_inference`
-- (on by default) and `cast_string_to_dynamic_use_inference` make the stored alternative depend on the row
-- contents, so the representative empty string says nothing about the runtime domain. Here the empty
-- string is stored as the `String` alternative, but a row `s = '42'` is stored as `UInt32`, and `length`
-- then throws `ILLEGAL_TYPE_OF_ARGUMENT` during TTL execution - so such casts keep the fail-closed path.
CREATE TABLE test_ttl_cast_string_to_variant_reject
(
    s String,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL d + INTERVAL 1 DAY DELETE WHERE length(CAST(s, 'Variant(String, UInt32, AggregateFunction(max, UInt32))')) > 3; -- { serverError BAD_TTL_EXPRESSION }

-- The same holds for a cast of a string to `Dynamic`, and through `Nullable`/`LowCardinality` wrappers and
-- container elements, which the cast recurses into.
CREATE TABLE test_ttl_cast_string_to_dynamic_reject
(
    s String,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL d + INTERVAL 1 DAY DELETE WHERE length(CAST(s, 'Dynamic')) > 3; -- { serverError BAD_TTL_EXPRESSION }

CREATE TABLE test_ttl_cast_nullable_string_to_dynamic_reject
(
    ns Nullable(String),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL d + INTERVAL 1 DAY DELETE WHERE length(CAST(ns, 'Dynamic')) > 3; -- { serverError BAD_TTL_EXPRESSION }

CREATE TABLE test_ttl_cast_lc_string_to_dynamic_reject
(
    ls LowCardinality(String),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL d + INTERVAL 1 DAY DELETE WHERE length(CAST(ls, 'Dynamic')) > 3; -- { serverError BAD_TTL_EXPRESSION }

CREATE TABLE test_ttl_cast_string_array_to_dynamic_reject
(
    arr Array(String),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL d + INTERVAL 1 DAY DELETE WHERE length(arrayElement(CAST(arr, 'Array(Dynamic)'), 1)) > 3; -- { serverError BAD_TTL_EXPRESSION }

SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_cast_string_to_variant_suspicious
(
    s String,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL d + INTERVAL 1 DAY DELETE WHERE length(CAST(s, 'Variant(String, UInt32, AggregateFunction(max, UInt32))')) > 3;

DROP TABLE test_ttl_cast_string_to_variant_suspicious;

SET allow_suspicious_ttl_expressions = 0;

-- A consumer that cannot handle the cast's actual payload type is still rejected: the runtime payload
-- of `CAST(n, 'Dynamic')` is `UInt32`, which `finalizeAggregation` cannot consume.
CREATE TABLE test_ttl_cast_plain_number_reject
(
    n UInt32,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL d + INTERVAL 1 DAY DELETE WHERE isNotNull(finalizeAggregation(CAST(n, 'Dynamic'))); -- { serverError BAD_TTL_EXPRESSION }

DROP TABLE IF EXISTS test_ttl_cast_variant_source_reject;

-- A `Variant` source is the exception to the "one representative value" rule: the cast preserves
-- whichever alternative each row stores, so the payload of the result is not fixed by a single
-- representative. Probing only the default (NULL) row would accept this expression, but a row storing
-- the `UInt32` alternative makes `length` throw `ILLEGAL_TYPE_OF_ARGUMENT` during TTL execution.
CREATE TABLE test_ttl_cast_variant_source_reject
(
    v Variant(String, UInt32),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL d + INTERVAL 1 DAY DELETE WHERE length(CAST(v, 'Dynamic')) > 3; -- { serverError BAD_TTL_EXPRESSION }

-- Narrowing is still exact for a `Variant` source: every alternative is probed, and a consumer that
-- handles all of them is accepted (`length` works on both `String` and `Array(UInt32)`), without being
-- confronted with synthetic payloads the cast can never produce.
CREATE TABLE test_ttl_cast_variant_source_accept
(
    v Variant(String, Array(UInt32)),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL d + INTERVAL 1 DAY DELETE WHERE length(CAST(v, 'Dynamic')) > 3;

DROP TABLE test_ttl_cast_variant_source_accept;

SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_cast_variant_source_suspicious
(
    v Variant(String, UInt32),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL d + INTERVAL 1 DAY DELETE WHERE length(CAST(v, 'Dynamic')) > 3;

DROP TABLE test_ttl_cast_variant_source_suspicious;

SET allow_suspicious_ttl_expressions = 0;

DROP TABLE IF EXISTS test_ttl_selector_same_domain_accept;

-- A selector function (`if`, `multiIf`, `coalesce`, `ifNull`) returns one of its value arguments, so a
-- non-constant condition can only choose *which* of their domains the result comes from. Both branches here
-- can only ever hold the `UInt32` payload, so the narrowed domain survives the selector and the expression
-- is accepted instead of being confronted with the synthetic payloads of a plain `Dynamic` column.
CREATE TABLE test_ttl_selector_same_domain_accept
(
    cond UInt8,
    n UInt32,
    m UInt32,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(if(cond, CAST(n, 'Dynamic'), CAST(m, 'Dynamic'))) + INTERVAL 1 DAY;

DROP TABLE test_ttl_selector_same_domain_accept;

DROP TABLE IF EXISTS test_ttl_selector_multi_if_same_domain_accept;

CREATE TABLE test_ttl_selector_multi_if_same_domain_accept
(
    cond UInt8,
    n UInt32,
    m UInt32,
    k UInt32,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(multiIf(cond, CAST(n, 'Dynamic'), cond > 1, CAST(m, 'Dynamic'), CAST(k, 'Dynamic'))) + INTERVAL 1 DAY;

DROP TABLE test_ttl_selector_multi_if_same_domain_accept;

DROP TABLE IF EXISTS test_ttl_selector_different_domains_reject;

-- The result of a selector carries the *union* of its branches' payload domains. A string-to-carrier cast
-- stays on the fail-closed static enumeration (it infers the stored payload from the row contents), so its
-- synthetic `AggregateFunction` candidate is in the union and keeps the whole selector rejected.
CREATE TABLE test_ttl_selector_different_domains_reject
(
    cond UInt8,
    n UInt32,
    s String,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(if(cond, CAST(n, 'Dynamic'), CAST(s, 'Dynamic'))) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_selector_different_domains_reject
(
    cond UInt8,
    n UInt32,
    s String,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(if(cond, CAST(n, 'Dynamic'), CAST(s, 'Dynamic'))) + INTERVAL 1 DAY;

DROP TABLE test_ttl_selector_different_domains_reject;

SET allow_suspicious_ttl_expressions = 0;

DROP TABLE IF EXISTS test_ttl_selector_literal_branches_accept;

-- Branches whose candidate materializations differ only by *value* still describe the same payload domain:
-- both literals below can only ever produce the `UInt8` payload, so the union of the branch domains is
-- propagated and the expression is accepted.
CREATE TABLE test_ttl_selector_literal_branches_accept
(
    cond UInt8,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(if(cond, CAST(1, 'Dynamic'), CAST(2, 'Dynamic'))) + INTERVAL 1 DAY;

DROP TABLE test_ttl_selector_literal_branches_accept;

DROP TABLE IF EXISTS test_ttl_selector_multi_if_literal_branches_accept;

CREATE TABLE test_ttl_selector_multi_if_literal_branches_accept
(
    cond UInt8,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(multiIf(cond, CAST(1, 'Dynamic'), cond > 1, CAST(2, 'Dynamic'), CAST(3, 'Dynamic'))) + INTERVAL 1 DAY;

DROP TABLE test_ttl_selector_multi_if_literal_branches_accept;

DROP TABLE IF EXISTS test_ttl_selector_union_of_domains_accept;

-- Branches with genuinely different payload domains are accepted when every payload in the *union* is
-- consumable: `toDateTime` handles both the `UInt32` payload of the first branch and the `UInt8` payload
-- of the second one.
CREATE TABLE test_ttl_selector_union_of_domains_accept
(
    cond UInt8,
    n UInt32,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(if(cond, CAST(n, 'Dynamic'), CAST(1, 'Dynamic'))) + INTERVAL 1 DAY;

DROP TABLE test_ttl_selector_union_of_domains_accept;

DROP TABLE IF EXISTS test_ttl_array_map_accept;

-- `arrayMap` returns an array of the values its lambda body produces, so the body's narrowed domain
-- describes the elements of the result: the elements of `arrayMap(x -> CAST(x, 'Dynamic'), arr)` over
-- `arr Array(UInt32)` can only ever hold the `UInt32` payload, and a consumer of an element is accepted.
CREATE TABLE test_ttl_array_map_accept
(
    arr Array(UInt32),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(arrayElement(arrayMap(x -> CAST(x, 'Dynamic'), arr), 1)) + INTERVAL 1 DAY;

DROP TABLE test_ttl_array_map_accept;

DROP TABLE IF EXISTS test_ttl_array_map_string_source_reject;

-- The rules narrowing the lambda body's domain are the same as everywhere else, so a cast of a *string*
-- inside the lambda stays fail-closed (`cast_string_to_dynamic_use_inference` would parse the stored
-- alternative out of the row contents) and the consumer of an element is rejected.
CREATE TABLE test_ttl_array_map_string_source_reject
(
    arr Array(String),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(arrayElement(arrayMap(x -> CAST(x, 'Dynamic'), arr), 1)) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

DROP TABLE IF EXISTS test_ttl_array_map_dynamic_input_reject;

-- A lambda body that just passes a stored `Dynamic` column through keeps the static enumeration of the
-- payloads that column can hold, so a consumer that cannot handle all of them is rejected.
CREATE TABLE test_ttl_array_map_dynamic_input_reject
(
    arr Array(Dynamic),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(arrayElement(arrayMap(x -> x, arr), 1)) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_array_map_dynamic_input_reject
(
    arr Array(Dynamic),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(arrayElement(arrayMap(x -> x, arr), 1)) + INTERVAL 1 DAY;

DROP TABLE test_ttl_array_map_dynamic_input_reject;

SET allow_suspicious_ttl_expressions = 0;

DROP TABLE IF EXISTS test_ttl_selector_lifted_branch_accept;

-- A selector converts every value branch to its result type, so a branch that is no carrier at all still
-- only contributes the payloads that conversion produces from its values: `if(cond, CAST(n, 'Dynamic'), m)`
-- over `n`, `m UInt32` holds a numeric payload whichever branch is taken, and `toDateTime` consumes it.
CREATE TABLE test_ttl_selector_lifted_branch_accept
(
    cond UInt8,
    n UInt32,
    m UInt32,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(if(cond, CAST(n, 'Dynamic'), m)) + INTERVAL 1 DAY;

DROP TABLE test_ttl_selector_lifted_branch_accept;

DROP TABLE IF EXISTS test_ttl_selector_lifted_literal_accept;

-- The same for a literal branch lifted to the carrier result type.
CREATE TABLE test_ttl_selector_lifted_literal_accept
(
    cond UInt8,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(if(cond, CAST(1, 'Dynamic'), 2)) + INTERVAL 1 DAY;

DROP TABLE test_ttl_selector_lifted_literal_accept;

DROP TABLE IF EXISTS test_ttl_selector_lifted_state_branch_reject;

-- Lifting a branch does not weaken the check: the aggregate state of the other branch stays in the union
-- of the domains, so a consumer that cannot handle it is still rejected.
CREATE TABLE test_ttl_selector_lifted_state_branch_reject
(
    cond UInt8,
    state AggregateFunction(max, UInt32),
    m UInt32,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(if(cond, CAST(state, 'Dynamic'), m)) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

DROP TABLE IF EXISTS test_ttl_selector_lifted_string_branch_reject;

-- A *string* branch is lifted by a conversion that infers the payload out of the row contents, so its
-- domain is unknown and the selector falls back to the static enumeration of the result type.
CREATE TABLE test_ttl_selector_lifted_string_branch_reject
(
    cond UInt8,
    n UInt32,
    s String,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(if(cond, CAST(n, 'Dynamic'), s)) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_selector_lifted_string_branch_reject
(
    cond UInt8,
    n UInt32,
    s String,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY tuple()
TTL toDateTime(if(cond, CAST(n, 'Dynamic'), s)) + INTERVAL 1 DAY;

DROP TABLE test_ttl_selector_lifted_string_branch_reject;

SET allow_suspicious_ttl_expressions = 0;
