-- Regression test: FunctionVariantAdaptor should handle Nothing type result
-- when a function like arrayElement is executed on Array(Nothing) inside a Variant context.
-- Previously this caused a logical error exception because Nothing type could not be cast to a Variant type.

SET enable_analyzer = 1;
SET allow_suspicious_variant_types = 1;
SET allow_suspicious_types_in_order_by = 1;

SELECT arrayElement(arr, 1) FROM (SELECT [(2, 'b'), (1, 'a')] AS arr UNION ALL SELECT [1, (3, 'c')] AS arr UNION ALL SELECT [] AS arr) ORDER BY arr;
SELECT arrayElement(arr, 2) FROM (SELECT [(2, 'b'), (1, 'a')] AS arr UNION ALL SELECT [1, (3, 'c')] AS arr UNION ALL SELECT [] AS arr) ORDER BY arr;
