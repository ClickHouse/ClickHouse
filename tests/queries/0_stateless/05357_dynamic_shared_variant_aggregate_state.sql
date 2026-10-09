-- Aggregate function states stored in the shared variant of `Dynamic` keep their values when read as a `Field`.

SELECT finalizeAggregation(tupleElement(`(x)`, 1)::AggregateFunction(groupArray, String)), finalizeAggregation(tupleElement(`(x)_1`, 1)::AggregateFunction(groupArray, String))
FROM (SELECT tuple(initializeAggregation('groupArrayState', 'x')::Dynamic(max_types = 0) AS x), tuple(initializeAggregation('groupArrayState', 'y')::Dynamic(max_types = 0) AS x));

SELECT initializeAggregation('countState', 1)::Dynamic(max_types = 0), initializeAggregation('countState', 1)::Dynamic(max_types = 0) FORMAT Null;
SELECT [initializeAggregation('countState', 1)]::Dynamic(max_types = 0), [initializeAggregation('countState', 1)]::Dynamic(max_types = 0) FORMAT Null;

SELECT finalizeAggregation(arr[1]::AggregateFunction(groupArray, String)), finalizeAggregation(arr[2]::AggregateFunction(groupArray, String))
FROM (SELECT groupArrayInsertAt(d, number) AS arr FROM (SELECT number, if(number = 0, initializeAggregation('groupArrayState', 'x'), initializeAggregation('groupArrayState', 'y'))::Dynamic(max_types = 0) AS d FROM numbers(2)));
SELECT finalizeAggregation(arr[1]::AggregateFunction(count, UInt8))
FROM (SELECT groupArrayInsertAt(d, number) AS arr FROM (SELECT number, initializeAggregation('countState', 1)::Dynamic(max_types = 0) AS d FROM numbers(1)));
SELECT groupArray(dynamicType(x)) FROM (SELECT arrayJoin(groupArrayInsertAt(tuple(toBool(number), initializeAggregation('countState', 1))::Dynamic(max_types = 0), number)) AS x FROM numbers(2));

-- Values of other types in the shared variant read back unchanged.
SELECT groupArray(toString(x)), groupArray(dynamicType(x)) FROM (SELECT arrayJoin(groupArrayInsertAt(toBool(number)::Dynamic(max_types = 0), number)) AS x FROM numbers(2));
SELECT toIntervalDay(1)::Dynamic(max_types = 0), toIntervalDay(1)::Dynamic(max_types = 0);
