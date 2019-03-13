set enable_debug_queries = 1;

DROP TABLE IF EXISTS a;
CREATE TABLE a (a UInt8, b UInt8) ENGINE MergeTree ORDER BY a;

ANALYZE SELECT * FROM a;

DROP TABLE a;


select '-- short_circuit_logic tests ---';
set enable_debug_queries = 1;
set allow_short_circuit_logic_expressions = 1;

analyze SELECT 1 AND (3 + 5) AND 0;

analyze SELECT number FROM numbers(10) where (1+2=3 or number = 3);
analyze SELECT number FROM numbers(10) where (1+1=3 or 1+2=2 or  number = 3);
analyze SELECT number FROM numbers(10) where (rand(10) = 1000 or 1>4);
analyze SELECT number FROM numbers(10) where (now() > 1000 or 1>4);
analyze SELECT number FROM numbers(10) where (1>2 or rand()>4);
analyze SELECT number FROM numbers(10) where (1=1 or rand()>4) as a;
analyze select rand(), rand(), rand() + rand();

analyze SELECT if(1=4 or (1+2=3 or 4=6), 33, 44);
analyze SELECT if(number = 44 and (1+2=3 or 4=6), 33, 44) FROM numbers(10);

analyze SELECT number FROM numbers(10) where (2 = 2) and (3 = 3) and (1 = 1);
analyze SELECT number FROM numbers(10) where 2 = 3 or 3 = 4 or ((number = 4 and  1 = 3));

analyze SELECT number FROM numbers(10) prewhere number % 2 = 1 and 3 % 2 = 0;
analyze SELECT number FROM numbers(10) where 2 = 3 or 3 = 4 or ((number = 4 and  1 = 3));
>>>>>>> simple logical_short_circuit optimization
