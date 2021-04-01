SELECT arrayFilter((a) -> ((a, arrayJoin([])) IN (Null, [Null])), []);
SELECT arrayFilter((a) -> ((a, arrayJoin([[]])) IN (Null, [Null])), []);

-- simplified from the https://clickhouse-test-reports.s3.yandex.net/10373/6c4748a63e7acde2cc3283d96ffec590aae1e724/fuzzer/fuzzer.log#fail1
SELECT * FROM system.one ARRAY JOIN arrayFilter((a) -> ((a, arrayJoin([])) IN (NULL)), []) AS arr_x; -- { serverError 43; }
SELECT * FROM numbers(1) LEFT ARRAY JOIN arrayFilter((x_0, x_1) -> (arrayJoin([]) IN (NULL)), [], []) AS arr_x;
