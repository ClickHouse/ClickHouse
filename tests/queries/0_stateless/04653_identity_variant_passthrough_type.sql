-- `identity` returns its argument column verbatim, so its declared type must stay the argument
-- type. Routing it through the Variant adaptor rewrote the declared type (dropping nested
-- LowCardinality, collapsing to Nullable(T)) while the column kept the original Variant, and
-- serializing the resulting block over the native protocol hit a bad cast.

SET allow_suspicious_variant_types = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET allow_suspicious_types_in_group_by = 1;

-- The declared type must be exactly the argument type.
SELECT toTypeName(identity(_CAST('x', 'Variant(String)')));
SELECT toTypeName(identity(_CAST('x', 'Variant(String, LowCardinality(String))')));
SELECT toTypeName(identity(_CAST('x', 'Variant(LowCardinality(String), UInt64)')));
SELECT toTypeName(identity(_CAST('1.5', 'Variant(LowCardinality(BFloat16), Point)')));
-- Custom type names of the alternatives survive (Point is a named Tuple(Float64, Float64) domain).
SELECT toTypeName(identity(_CAST('x', 'Variant(LowCardinality(String), Point, Ring)')));
-- No LowCardinality anywhere: unchanged.
SELECT toTypeName(identity(_CAST('x', 'Variant(String, UInt64)')));
-- Same base class, so __scalarSubqueryResult is covered too.
SELECT toTypeName(__scalarSubqueryResult(_CAST('x', 'Variant(String, LowCardinality(String))')));
-- Non-Variant arguments keep their existing behaviour.
SELECT toTypeName(identity(1::UInt8)), toTypeName(identity('s'::LowCardinality(String))), toTypeName(identity(1::Nullable(UInt8)));
-- Variant nested inside a container was already correct.
SELECT toTypeName(identity(_CAST(['x'], 'Array(Variant(LowCardinality(String), UInt64))')));

-- The reported failure: aggregating over the passthrough expression and sending the result.
DROP TABLE IF EXISTS t_identity_variant;
CREATE TABLE t_identity_variant (k Int32) ENGINE = MergeTree ORDER BY k AS SELECT number FROM numbers(5);

SELECT identity(_CAST('x', 'Variant(LowCardinality(String), UInt64)')) AS e, count() FROM t_identity_variant GROUP BY e;
SELECT identity(_CAST('x', 'Variant(String, LowCardinality(String))')) AS e, count() FROM t_identity_variant GROUP BY e;
SELECT identity(_CAST('x', 'Variant(String)')) AS e, count() FROM t_identity_variant GROUP BY e;
SELECT identity(_CAST('1.5', 'Variant(LowCardinality(BFloat16), Point)')) AS e, variantType(e), count() FROM t_identity_variant GROUP BY e;
-- The exact type list from the reported query. The prefix of the state is written for every
-- alternative, so the LowCardinality one does not have to be the populated one.
SELECT identity(_CAST('2020-01-02 03:04:05', 'Variant(BFloat16, DateTime64(5, \'UTC\'), Int128, LowCardinality(BFloat16), Point, String)')) AS e, variantType(e), count()
FROM t_identity_variant GROUP BY e SETTINGS enable_time_time64_type = 1;
SELECT identity(_CAST('x', 'Variant(String, UInt64)')) AS e, count() FROM t_identity_variant GROUP BY e;

-- The value survives a round trip through the native serialization, covering the read side.
DROP TABLE IF EXISTS t_identity_variant_rt;
CREATE TABLE t_identity_variant_rt (e Variant(LowCardinality(String), UInt64), c UInt64) ENGINE = MergeTree ORDER BY c;
INSERT INTO t_identity_variant_rt SELECT identity(_CAST('x', 'Variant(LowCardinality(String), UInt64)')) AS e, count() FROM t_identity_variant GROUP BY e;
SELECT e, c, toTypeName(e), variantType(e) FROM t_identity_variant_rt;

-- LowCardinality(Nullable(...)) is still rejected inside Variant.
SELECT toTypeName(identity(_CAST('x', 'Variant(LowCardinality(Nullable(String)), UInt64)'))); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_identity_variant_rt;
DROP TABLE t_identity_variant;
