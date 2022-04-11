SELECT finalizeAggregation(initializeAggregation('uniqExactState', toNullable('foo')));
SELECT toTypeName(initializeAggregation('uniqExactState', toNullable('foo')));

SELECT finalizeAggregation(initializeAggregation('uniqExactState', toNullable(123)));
SELECT toTypeName(initializeAggregation('uniqExactState', toNullable(123)));

SELECT initializeAggregation('uniqExactState', toNullable('foo')) = arrayReduce('uniqExactState', [toNullable('foo')]);
SELECT initializeAggregation('uniqExactState', toNullable(123)) = arrayReduce('uniqExactState', [toNullable(123)]);
