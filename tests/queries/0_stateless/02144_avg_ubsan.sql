-- { echo }

-- Aggregate function 'avg' allows overflow with two's complement arithmetics.
-- This contradicts the standard SQL semantic and we are totally fine with it.

-- AggregateFunctionAvg::add
SELECT avg(-8000000000000000000) FROM (SELECT *, 1 AS k FROM numbers(65535*2)) GROUP BY k;
-- AggregateFunctionAvg::addBatchSinglePlace
SELECT avg(-8000000000000000000) FROM numbers(65535 * 2);
-- AggregateFunctionAvg::addBatchSinglePlaceNotNull
SELECT avg(toNullable(-8000000000000000000)) FROM numbers(65535 * 2);
