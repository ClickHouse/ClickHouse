SELECT number % 2 ? toDateTime('2000-01-01 00:00:00') : toDateTime('2001-02-03 04:05:06') FROM numbers(2);
SELECT number % 2 ? toDateTime('2000-01-01 00:00:00') : materialize(toDateTime('2001-02-03 04:05:06')) FROM numbers(2);
SELECT number % 2 ? materialize(toDateTime('2000-01-01 00:00:00')) : toDateTime('2001-02-03 04:05:06') FROM numbers(2);
SELECT number % 2 ? materialize(toDateTime('2000-01-01 00:00:00')) : materialize(toDateTime('2001-02-03 04:05:06')) FROM numbers(2);

SELECT number % 2 ? toDate('2000-01-01') : toDate('2001-02-03') FROM numbers(2);
SELECT number % 2 ? toDate('2000-01-01') : materialize(toDate('2001-02-03')) FROM numbers(2);
SELECT number % 2 ? materialize(toDate('2000-01-01')) : toDate('2001-02-03') FROM numbers(2);
SELECT number % 2 ? materialize(toDate('2000-01-01')) : materialize(toDate('2001-02-03')) FROM numbers(2);
