SELECT 1 as a, count() FROM numbers(10) WHERE 0 GROUP BY a;
SELECT count() FROM numbers(10) WHERE 0;

SELECT 1 as a, count() FROM numbers(10) WHERE 0 GROUP BY a SETTINGS empty_result_for_aggregation_by_constant_keys_on_empty_set = 0;
