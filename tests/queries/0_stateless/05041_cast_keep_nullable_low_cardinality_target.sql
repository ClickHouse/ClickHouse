-- Test for https://github.com/ClickHouse/ClickHouse/issues/103485

SET cast_keep_nullable = 1;

-- A LowCardinality target keeps nullability as LowCardinality(Nullable(T)).
SELECT toTypeName(NULL::Nullable(String)::LowCardinality(String)), NULL::Nullable(String)::LowCardinality(String);
SELECT toTypeName(NULL::LowCardinality(Nullable(String))::LowCardinality(String)), NULL::LowCardinality(Nullable(String))::LowCardinality(String);
SELECT toTypeName(NULL::Nullable(String)::LowCardinality(FixedString(5))), NULL::Nullable(String)::LowCardinality(FixedString(5));

SELECT toTypeName(NULL::Nullable(UInt16)::LowCardinality(UInt16)), NULL::Nullable(UInt16)::LowCardinality(UInt16) SETTINGS allow_suspicious_low_cardinality_types = 1;

-- Non-NULL values through the same casts carry the wrapper too.
SELECT toTypeName('hello'::Nullable(String)::LowCardinality(String)), 'hello'::Nullable(String)::LowCardinality(String);
SELECT toTypeName('hello'::LowCardinality(Nullable(String))::LowCardinality(String)), 'hello'::LowCardinality(Nullable(String))::LowCardinality(String);

-- An already nullable target is unchanged.
SELECT toTypeName(NULL::Nullable(String)::LowCardinality(Nullable(String))), NULL::Nullable(String)::LowCardinality(Nullable(String));

-- Targets that are not LowCardinality keep their previous behaviour.
SELECT toTypeName(NULL::Nullable(String)::String), NULL::Nullable(String)::String;
SELECT toTypeName(NULL::LowCardinality(Nullable(String))::String), NULL::LowCardinality(Nullable(String))::String;
SELECT toTypeName(NULL::Nullable(String)::Tuple(String));

-- Dynamic and Variant sources reach the LowCardinality target through a separate
-- conversion path, which stops throwing once the result type can hold NULL.
SELECT toTypeName(CAST(NULL::Dynamic, 'LowCardinality(String)')), CAST(NULL::Dynamic, 'LowCardinality(String)');
SELECT toTypeName(CAST(CAST(NULL, 'Variant(String, UInt8)'), 'LowCardinality(String)')), CAST(CAST(NULL, 'Variant(String, UInt8)'), 'LowCardinality(String)');

-- Array has no nullable representation to move the NULL into, so it still rejects it.
SELECT NULL::Nullable(String)::Array(String); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

-- Negative controls.
SELECT toTypeName('h'::String::LowCardinality(String)), 'h'::String::LowCardinality(String);
SELECT toTypeName(_CAST(NULL::Nullable(String), 'LowCardinality(String)')); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

SET cast_keep_nullable = 0;

SELECT NULL::Nullable(String)::LowCardinality(String); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT toTypeName('hello'::Nullable(String)::LowCardinality(String)), 'hello'::Nullable(String)::LowCardinality(String);
