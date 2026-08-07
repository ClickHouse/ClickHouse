SELECT uniqExact(x) FROM (SELECT uniqState(number) AS x FROM numbers(100));
SELECT uniqExact(x) FROM (SELECT uniqState(number) AS x FROM numbers(1000));
SELECT hex(toString(uniqExactState(x))) FROM (SELECT uniqState(number) AS x FROM numbers(1000));
SELECT hex(toString(uniqExactState(x))) FROM (SELECT quantileState(number) AS x FROM numbers(1000));
SELECT toTypeName(uniqExactState(x)) FROM (SELECT quantileState(number) AS x FROM numbers(1000));
SELECT toTypeName(initializeAggregation('uniqExact', 0));
SELECT toTypeName(initializeAggregation('uniqExactState', 0));
SELECT toTypeName(initializeAggregation('uniqExactState', initializeAggregation('quantileState', 0)));
SELECT hex(toString(initializeAggregation('quantileState', 0)));
SELECT toTypeName(initializeAggregation('sumState', initializeAggregation('quantileState', 0))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toTypeName(initializeAggregation('anyState', initializeAggregation('quantileState', 0)));
SELECT toTypeName(initializeAggregation('anyState', initializeAggregation('uniqState', 0)));
SELECT hex(toString(initializeAggregation('uniqState', initializeAggregation('uniqState', 0))));
SELECT hex(toString(initializeAggregation('uniqState', initializeAggregation('quantileState', 0))));
SELECT hex(toString(initializeAggregation('anyLastState', initializeAggregation('uniqState', 0))));
SELECT hex(toString(initializeAggregation('anyState', initializeAggregation('uniqState', 0))));
SELECT hex(toString(initializeAggregation('maxState', initializeAggregation('uniqState', 0)))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hex(toString(initializeAggregation('uniqExactState', initializeAggregation('uniqState', 0))));
SELECT finalizeAggregation(initializeAggregation('uniqExactState', initializeAggregation('uniqState', 0)));
SELECT toTypeName(quantileState(x)) FROM (SELECT uniqState(number) AS x FROM numbers(1000)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hex(toString(quantileState(x))) FROM (SELECT uniqState(number) AS x FROM numbers(1000)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hex(toString(anyState(x))), hex(toString(any(x))) FROM (SELECT uniqState(number) AS x FROM numbers(1000)) FORMAT Vertical;
