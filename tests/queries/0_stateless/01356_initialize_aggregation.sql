SELECT uniqMerge(state) FROM (SELECT initializeAggregation('uniqState', number % 3) AS state FROM system.numbers LIMIT 10000);
SELECT topKWeightedMerge(10)(state) FROM (SELECT initializeAggregation('topKWeightedState(10)', number, number) AS state FROM system.numbers LIMIT 1000);
SELECT topKWeightedMerge(10)(state) FROM (SELECT initializeAggregation('topKWeightedState(10)', 1, number) AS state FROM system.numbers LIMIT 1000);
-- order in libstdc++ and libc++ may be a little different, that's why we need arraySort()
SELECT arraySort(topKWeightedMerge(10)(state)) FROM (SELECT initializeAggregation('topKWeightedState(10)', number, 1) AS state FROM system.numbers LIMIT 1000);
