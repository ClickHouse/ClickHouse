SET enable_analyzer = 1;
SET aggregate_functions_null_for_empty = 1;

SELECT max(aggr) FROM (SELECT max('92233720368547758.06') AS aggr FROM system.one);
