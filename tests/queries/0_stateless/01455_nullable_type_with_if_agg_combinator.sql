-- Value nullable
SELECT anyIf(CAST(number, 'Nullable(UInt8)'), number = 3) AS a, toTypeName(a) FROM numbers(2);
-- Value and condition nullable
SELECT anyIf(number, number = 3) AS a, toTypeName(a) FROM (SELECT CAST(number, 'Nullable(UInt8)') AS number FROM numbers(2));
-- Condition nullable
SELECT anyIf(CAST(number, 'UInt8'), number = 3) AS a, toTypeName(a) FROM (SELECT CAST(number, 'Nullable(UInt8)') AS number FROM numbers(2));
