SET optimize_if_transform_const_strings_to_lowcardinality = 1;
SET optimize_if_transform_strings_to_enum = 0;

SELECT 'Typed all-NULL constants do not get LowCardinality (no actual string value)';
SELECT if(number % 2 = 0, CAST(NULL AS Nullable(String)), NULL) AS res, toTypeName(res) FROM numbers(2);
SELECT if(number % 2 = 0, CAST(NULL AS Nullable(String)), CAST(NULL AS Nullable(String))) AS res, toTypeName(res) FROM numbers(1);
SELECT multiIf(number % 2 = 0, CAST(NULL AS Nullable(String)), NULL) AS res, toTypeName(res) FROM numbers(2);
SELECT multiIf(number % 2 = 0, CAST(NULL AS Nullable(String)), number = 1, NULL, CAST(NULL AS Nullable(String))) AS res, toTypeName(res) FROM numbers(2);
SELECT transform(number, [1], [CAST(NULL AS Nullable(String))], CAST(NULL AS Nullable(String))) AS res, toTypeName(res) FROM numbers(2);

SELECT 'At least one non-NULL string constant still gets LowCardinality';
SELECT if(number % 2 = 0, 'a', CAST(NULL AS Nullable(String))) AS res, toTypeName(res) FROM numbers(2);
SELECT if(number % 2 = 0, CAST('a' AS Nullable(String)), NULL) AS res, toTypeName(res) FROM numbers(2);
SELECT multiIf(number % 2 = 0, CAST(NULL AS Nullable(String)), number = 1, 'b', NULL) AS res, toTypeName(res) FROM numbers(2);
SELECT transform(number, [1, 2], ['one', CAST(NULL AS Nullable(String))], CAST(NULL AS Nullable(String))) AS res, toTypeName(res) FROM numbers(3);
SELECT transform(number, [1], [CAST(NULL AS Nullable(String))], 'other') AS res, toTypeName(res) FROM numbers(2);
