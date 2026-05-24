SELECT uniqExact(nan) FROM numbers(1000);
SELECT uniqExact(number + nan) FROM numbers(1000);
SELECT sumDistinct(number + nan) FROM numbers(1000);
SELECT DISTINCT number + nan FROM numbers(1000);

SELECT topKWeightedMerge(1)(initializeAggregation('topKWeightedState(1)', nan, arrayJoin(range(10))));

select number + nan k from numbers(256) group by k;

-- Constructs 10 different NaN bit patterns by perturbing the mantissa of `nan`.
-- Since #105748 (DISTINCT NaN canonicalization), hash-based set semantics fold
-- every NaN bit pattern onto a single canonical NaN, so all 10 collapse to 1.
SELECT uniqExact(reinterpretAsFloat64(reinterpretAsFixedString(reinterpretAsUInt64(reinterpretAsFixedString(nan)) + number))) FROM numbers(10);
