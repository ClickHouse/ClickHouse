set enable_debug_queries = 1;

DROP TABLE IF EXISTS a;
CREATE TABLE a (a UInt8, b UInt8) ENGINE MergeTree ORDER BY a;

ANALYZE SELECT * FROM a;

DROP TABLE a;


select '-- short_circuit_logic tests ---';
set enable_debug_queries = 1;
set allow_short_circuit_logic_expressions = 1;

analyze SELECT 1 AND (3 + 5) AND 0;

analyze SELECT number FROM numbers(10) WHERE (1+2=3 OR number = 3);
analyze SELECT number FROM numbers(10) WHERE (1+1=3 OR 1+2=2 OR  number = 3);
analyze SELECT number FROM numbers(10) WHERE (rand(10) = 1000 OR 1>4);
analyze SELECT number FROM numbers(10) WHERE (now() > 1000 OR 1>4);
analyze SELECT number FROM numbers(10) WHERE (1>2 OR rand()>4);
analyze SELECT number FROM numbers(10) WHERE (1=1 OR rand()>4) as a;
analyze select rand(), rand(), rand() + rand();

analyze SELECT if(1=4 OR (1+2=3 OR 4=6), 33, 44);
analyze SELECT if(number = 44 AND (1+2=3 OR 4=6), 33, 44) FROM numbers(10);

analyze SELECT number FROM numbers(10) WHERE (2 = 2) AND (3 = 3) AND (1 = 1);
analyze SELECT number FROM numbers(10) WHERE 2 = 3 OR 3 = 4 OR ((number = 4 AND  1 = 3));

<<<<<<< HEAD
analyze SELECT number FROM numbers(10) prewhere number % 2 = 1 and 3 % 2 = 0;
analyze SELECT number FROM numbers(10) where 2 = 3 or 3 = 4 or ((number = 4 and  1 = 3));
>>>>>>> simple logical_short_circuit optimization
=======
analyze SELECT number FROM numbers(10) PREWHERE number % 2 = 1 AND 3 % 2 = 0;
analyze SELECT number FROM numbers(10) WHERE 2 = 3 OR 3 = 4 OR ((number = 4 AND  1 = 3));
<<<<<<< HEAD
>>>>>>> fix sql keywords to upper
=======

analyze SELECT [1, 2, 3 + 3 = 6 ? 7 : 8];
analyze SELECT arrayMap(x -> 1 = 1 or x + 3 = 6 ? 3 : 4,  range(10));
analyze SELECT arrayMap(x -> (3 + 3), range(10 + 3));
>>>>>>> support lambda function optimize
