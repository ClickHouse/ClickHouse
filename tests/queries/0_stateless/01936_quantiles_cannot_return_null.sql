set aggregate_functions_null_for_empty=0;

SELECT quantiles(0.95)(x) FROM (SELECT 1 x WHERE 0);
SELECT quantiles(0.95)(number) FROM (SELECT number FROM numbers(10) WHERE number > 10);

set aggregate_functions_null_for_empty=1;

SELECT quantiles(0.95)(x) FROM (SELECT 1 x WHERE 0);
SELECT quantiles(0.95)(number) FROM (SELECT number FROM numbers(10) WHERE number > 10);
