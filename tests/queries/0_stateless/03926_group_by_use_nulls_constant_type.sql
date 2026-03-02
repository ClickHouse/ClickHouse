-- Reproduce an exception in constant folding when group_by_use_nulls wraps GROUP BY key
-- constants in Nullable, but a literal constant with the same name in the SELECT expression
-- should keep its original non-nullable type.

SELECT DISTINCT (2, isNull(materialize(1))) = assumeNotNull((1, 2)) GROUP BY 1 WITH ROLLUP HAVING materialize(2) SETTINGS group_by_use_nulls = 1, enable_positional_arguments = 0;
