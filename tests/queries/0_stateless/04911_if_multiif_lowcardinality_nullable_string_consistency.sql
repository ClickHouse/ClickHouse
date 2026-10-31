SET optimize_if_transform_const_strings_to_lowcardinality = 1;
SET optimize_if_transform_strings_to_enum = 0;

SELECT 'Explicit Nullable(String) constant branches count as strings';
SELECT toTypeName(res), res FROM (SELECT if(number % 2 = 0, CAST('a' AS Nullable(String)), CAST('b' AS Nullable(String))) AS res FROM numbers(2));
SELECT toTypeName(res), res FROM (SELECT if(number % 2 = 0, 'a', CAST('b' AS Nullable(String))) AS res FROM numbers(2));
SELECT toTypeName(res), res FROM (SELECT if(number % 2 = 0, CAST(NULL AS Nullable(String)), 'b') AS res FROM numbers(2));
SELECT toTypeName(res), res FROM (SELECT multiIf(number % 3 = 0, CAST('a' AS Nullable(String)), number % 3 = 1, 'b', NULL) AS res FROM numbers(3));
SELECT toTypeName(res), res FROM (SELECT multiIf(number % 2 = 0, CAST('a' AS Nullable(String)), CAST('b' AS Nullable(String))) AS res FROM numbers(2));

SELECT 'Only-NULL branches keep Nullable(Nothing)';
SELECT toTypeName(if(number % 2 = 0, NULL, NULL)) FROM numbers(1);
SELECT toTypeName(multiIf(number % 2 = 0, NULL, NULL)) FROM numbers(1);

SELECT 'Explicit LowCardinality branches do not leak into Variant';
SET use_variant_as_common_type = 1;
SELECT toTypeName(res), res FROM (SELECT multiIf(number % 3 = 0, range(number + 1), number % 3 = 1, number, toLowCardinality('x')) AS res FROM numbers(3));
SELECT toTypeName(res), res FROM (SELECT if(number % 2 = 0, number, toLowCardinality('x')) AS res FROM numbers(2));
SET use_variant_as_common_type = 0;

SELECT 'Constant LowCardinality(String) branches produce the same type in if and multiIf';
SELECT toTypeName(if(number % 2 = 0, toLowCardinality('a'), 'b')), toTypeName(multiIf(number % 2 = 0, toLowCardinality('a'), 'b')) FROM numbers(1);

SELECT 'With the optimization disabled, non-constant LowCardinality branches follow the pre-existing contract';
SET optimize_if_transform_const_strings_to_lowcardinality = 0;
SELECT toTypeName(if(number % 2 = 0, materialize(toLowCardinality('a')), 'b')), toTypeName(multiIf(number % 2 = 0, materialize(toLowCardinality('a')), 'b')) FROM numbers(1);
SELECT toTypeName(if(number % 2 = 0, CAST('a' AS Nullable(String)), CAST('b' AS Nullable(String)))), toTypeName(multiIf(number % 2 = 0, CAST('a' AS Nullable(String)), CAST('b' AS Nullable(String)))) FROM numbers(1);
