SET use_legacy_to_time = 0;

SELECT number % 2 ? toTime('00:00:00') : toTime('04:05:06') FROM numbers(2);
SELECT number % 2 ? toTime('00:00:00') : materialize(toTime('04:05:06')) FROM numbers(2);
SELECT number % 2 ? materialize(toTime('00:00:00')) : toTime('04:05:06') FROM numbers(2);
SELECT number % 2 ? materialize(toTime('00:00:00')) : materialize(toTime('04:05:06')) FROM numbers(2);

SELECT number % 2 ? toTime64('00:00:00', 2) : toTime64('04:05:06', 2) FROM numbers(2);
SELECT number % 2 ? toTime64('00:00:00', 2) : materialize(toTime64('04:05:06', 2)) FROM numbers(2);
SELECT number % 2 ? materialize(toTime64('00:00:00', 2)) : toTime64('04:05:06', 2) FROM numbers(2);
SELECT number % 2 ? materialize(toTime64('00:00:00', 2)) : materialize(toTime64('04:05:06', 2)) FROM numbers(2);
