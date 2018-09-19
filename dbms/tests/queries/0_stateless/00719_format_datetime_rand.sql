WITH toDateTime(rand()) AS t SELECT count() FROM numbers(1000000) WHERE formatDateTime(t, '%F %T') != toString(t);
WITH toDateTime(rand()) AS t SELECT count() FROM numbers(1000000) WHERE formatDateTime(t, '%Y-%m-%d %H:%M:%S') != toString(t);
WITH toDateTime(rand()) AS t SELECT count() FROM numbers(1000000) WHERE formatDateTime(t, '%Y-%m-%d %R:%S') != toString(t);
WITH toDateTime(rand()) AS t SELECT count() FROM numbers(1000000) WHERE formatDateTime(t, '%F %R:%S') != toString(t);

WITH toDate(today() + rand() % 4096) AS t SELECT count() FROM numbers(1000000) WHERE formatDateTime(t, '%F') != toString(t);
WITH toDate(today() + rand() % 4096) AS t SELECT count() FROM numbers(1000000) WHERE formatDateTime(t, '%F %T') != toString(toDateTime(t));
