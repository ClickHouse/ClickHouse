-- Aggregate function 'sum' allows overflow with two's complement arithmetics.
-- This contradicts the standard SQL semantic and we are totally fine with it.
SELECT sum(-8000000000000000000) FROM numbers(11);
