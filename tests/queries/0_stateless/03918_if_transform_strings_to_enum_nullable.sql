-- Regression test: IfTransformStringsToEnumPass should not crash when
-- the `if` result type is Nullable(String) (e.g. due to GROUP BY WITH CUBE
-- and group_by_use_nulls = true).

SET optimize_if_transform_strings_to_enum = 1;
SET group_by_use_nulls = 1;

SELECT DISTINCT if(number < 5, 'google', 'censor.net') AS value, value FROM system.numbers GROUP BY and(isZeroOrNull(10), *), 2 WITH CUBE LIMIT 0;
