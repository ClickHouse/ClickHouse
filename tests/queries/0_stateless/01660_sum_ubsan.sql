-- Aggregate function 'sum' allows overflow with two's complement arithmetics.
-- This contradicts the standard SQL semantic and we are totally fine with it.
SELECT sum(-8000000000000000000) FROM numbers(11);
SELECT avg(-8000000000000000000) FROM numbers(11);

SELECT sum(-8000000000000000000) FROM remote('127.0.0.{1,2,3,4,5,6,7,8,9,10,11}', system.one);
SELECT avg(-8000000000000000000) FROM remote('127.0.0.{1,2,3,4,5,6,7,8,9,10,11}', system.one);

SELECT sumKahan(-8000000000000000000) FROM numbers(11);
