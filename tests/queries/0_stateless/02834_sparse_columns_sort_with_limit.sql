DROP TABLE IF EXISTS t_sparse_sort_limit;

CREATE TABLE t_sparse_sort_limit (date Date, i UInt64, v Int16)
ENGINE = MergeTree ORDER BY (date, i)
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_sparse_sort_limit SELECT '2020-10-10', number % 10, number FROM numbers(100000);
INSERT INTO t_sparse_sort_limit SELECT '2020-10-11', number % 10, number FROM numbers(100000);

SELECT count() FROM (SELECT toStartOfMonth(date) AS d FROM t_sparse_sort_limit ORDER BY -i LIMIT 65536);

DROP TABLE IF EXISTS t_sparse_sort_limit;
