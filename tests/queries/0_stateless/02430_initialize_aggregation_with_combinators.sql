select initializeAggregation('uniqStateMap', map(1, 2));
select initializeAggregation('uniqStateForEach', [1, 2]);
select initializeAggregation('uniqStateForEachMapForEach', [map(1, [2])]);

