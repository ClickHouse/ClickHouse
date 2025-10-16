SET optimize_rewrite_aggregate_function_with_if=1;

SELECT sum(CASE WHEN dummy = 1 THEN 0 ELSE null END) FROM remote('127.0.0.{1,2}', system.one);
SELECT sum(if(dummy = 0, 5, null)) FROM remote('127.0.0.{1,2}', system.one);
