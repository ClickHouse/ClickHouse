SET enable_analyzer = 1;

SELECT number FROM numbers(untuple(tuple(1)));

SELECT '--';

SELECT number FROM numbers(untuple(tuple(0, 2)));

SELECT '--';

SELECT number FROM numbers(untuple(tuple(1, 2)));

SELECT '--';

SELECT cast(tuple(1), 'Tuple(value UInt64)') AS value, number FROM numbers(untuple(value));

SELECT '--';

SELECT cast(tuple(0, 1), 'Tuple(value_1 UInt64, value_2 UInt64)') AS value, number FROM numbers(untuple(value));

SELECT '--';

SELECT cast(tuple(1, 2), 'Tuple(value_1 UInt64, value_2 UInt64)') AS value, number FROM numbers(untuple(value));

SELECT '--';

SELECT cast(tuple(1), 'Tuple(value UInt64)') AS value, number FROM numbers(value.*);

SELECT '--';

SELECT cast(tuple(0, 1), 'Tuple(value_1 UInt64, value_2 UInt64)') AS value, number FROM numbers(value.*);

SELECT '--';

SELECT cast(tuple(1, 2), 'Tuple(value_1 UInt64, value_2 UInt64)') AS value, number FROM numbers(value.*);

SELECT '--';

SELECT cast(tuple('1'), 'Tuple(value String)') AS value, number FROM numbers(value.* APPLY x -> toUInt64(x));

SELECT '--';

SELECT cast(tuple('0', '1'), 'Tuple(value_1 String, value_2 String)') AS value, number FROM numbers(value.* APPLY x -> toUInt64(x));

SELECT '--';

SELECT cast(tuple('1', '2'), 'Tuple(value_1 String, value_2 String)') AS value, number FROM numbers(value.* APPLY x -> toUInt64(x));
