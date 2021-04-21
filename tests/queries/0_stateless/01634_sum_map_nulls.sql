SELECT initializeAggregation('sumMap', [1, 2, 1], [1, 1, 1], [-1, null, 10]);
SELECT initializeAggregation('sumMap', [1, 2, 1], [1, 1, 1], [-1, null, null]);
SELECT initializeAggregation('sumMap', [1, 2, 1], [1, 1, 1], [null, null, null]); -- { serverError 43 }
SELECT initializeAggregation('sumMap', [1, 2, 1], [1, 1, 1], [-1, 10, 10]);
SELECT initializeAggregation('sumMap', [1, 2, 1], [1, 1, 1], [-1, 10, null]);
