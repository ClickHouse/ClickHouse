-- Regression test: -ForEach with -OrDefault/-Distinct nested combinators
-- must not trigger UBSan misaligned-address errors.
-- The issue was that AggregateFunctionOrFill::sizeOfData() returned an
-- unpadded value, so the second element in the ForEach array of aggregate
-- states was misaligned.

SELECT countOrDefaultDistinctForEach([1, 2, 3]);
SELECT sumOrDefaultDistinctForEach([10, 20]);
SELECT sumOrNullForEach([100, 200, 300]);
