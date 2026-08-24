SELECT finalizeAggregation(initializeAggregation('uniqExactState', toNullable('foo')));
SELECT toTypeName(initializeAggregation('uniqExactState', toNullable('foo')));

SELECT finalizeAggregation(initializeAggregation('uniqExactState', toNullable(123)));
SELECT toTypeName(initializeAggregation('uniqExactState', toNullable(123)));

SELECT toTypeName(initializeAggregation('uniqExactState', toNullable('foo'))) = toTypeName(arrayReduce('uniqExactState', [toNullable('foo')]));
SELECT toTypeName(initializeAggregation('uniqExactState', toNullable(123))) = toTypeName(arrayReduce('uniqExactState', [toNullable(123)]));
