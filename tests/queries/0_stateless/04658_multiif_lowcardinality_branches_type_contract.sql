-- Pin the output type contract of `multiIf` / `if` for explicit `LowCardinality` branch arguments.
-- `multiIf` opts out of the default `LowCardinality` implementation (`useDefaultImplementationForLowCardinalityColumns`
-- returns false, see src/Functions/multiIf.cpp) and re-implements its behavior by hand: since
-- `canBeExecutedOnLowCardinalityDictionary` has always been false for `multiIf`, the default implementation
-- never wrapped the result back into `LowCardinality`, so non-constant `LowCardinality(String)` branches
-- have always produced a plain `String` result. These queries pin that contract under both values of
-- `optimize_if_transform_const_strings_to_lowcardinality`; the setting only upgrades the result of
-- constant string branches to `LowCardinality(String)`.

SET optimize_if_transform_strings_to_enum = 0;

SELECT '-- optimize_if_transform_const_strings_to_lowcardinality = 1';
SET optimize_if_transform_const_strings_to_lowcardinality = 1;

-- Non-constant LowCardinality branches: plain String result, as with the default implementation.
SELECT toTypeName(multiIf(number % 2, toLowCardinality(materialize('a')), toLowCardinality(materialize('b')))) AS t, multiIf(number % 2, toLowCardinality(materialize('a')), toLowCardinality(materialize('b')))
FROM numbers(2);
SELECT toTypeName(if(number % 2, toLowCardinality(materialize('a')), toLowCardinality(materialize('b')))) AS t, if(number % 2, toLowCardinality(materialize('a')), toLowCardinality(materialize('b')))
FROM numbers(2);

-- Non-string LowCardinality branches are unaffected by the constant-string optimization.
SELECT toTypeName(multiIf(number % 2, toLowCardinality(materialize(1 :: UInt64)), toLowCardinality(materialize(2 :: UInt64)))) AS t, multiIf(number % 2, toLowCardinality(materialize(1 :: UInt64)), toLowCardinality(materialize(2 :: UInt64)))
FROM numbers(2);

-- Constant LowCardinality string branches take the constant-string optimization.
SELECT toTypeName(multiIf(number % 2, toLowCardinality('a'), toLowCardinality('b'))) AS t, multiIf(number % 2, toLowCardinality('a'), toLowCardinality('b'))
FROM numbers(2);

-- A LowCardinality branch must not leak into a Variant common type.
SELECT toTypeName(multiIf(number % 2, toLowCardinality(materialize('a')), materialize(42))) AS t, multiIf(number % 2, toLowCardinality(materialize('a')), materialize(42))
FROM numbers(2)
SETTINGS use_variant_as_common_type = 1;

SELECT '-- optimize_if_transform_const_strings_to_lowcardinality = 0';
SET optimize_if_transform_const_strings_to_lowcardinality = 0;

SELECT toTypeName(multiIf(number % 2, toLowCardinality(materialize('a')), toLowCardinality(materialize('b')))) AS t, multiIf(number % 2, toLowCardinality(materialize('a')), toLowCardinality(materialize('b')))
FROM numbers(2);
SELECT toTypeName(if(number % 2, toLowCardinality(materialize('a')), toLowCardinality(materialize('b')))) AS t, if(number % 2, toLowCardinality(materialize('a')), toLowCardinality(materialize('b')))
FROM numbers(2);
SELECT toTypeName(multiIf(number % 2, toLowCardinality(materialize(1 :: UInt64)), toLowCardinality(materialize(2 :: UInt64)))) AS t, multiIf(number % 2, toLowCardinality(materialize(1 :: UInt64)), toLowCardinality(materialize(2 :: UInt64)))
FROM numbers(2);
SELECT toTypeName(multiIf(number % 2, toLowCardinality('a'), toLowCardinality('b'))) AS t, multiIf(number % 2, toLowCardinality('a'), toLowCardinality('b'))
FROM numbers(2);
SELECT toTypeName(multiIf(number % 2, toLowCardinality(materialize('a')), materialize(42))) AS t, multiIf(number % 2, toLowCardinality(materialize('a')), materialize(42))
FROM numbers(2)
SETTINGS use_variant_as_common_type = 1;
