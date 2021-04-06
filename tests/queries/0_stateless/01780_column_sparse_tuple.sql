DROP TABLE IF EXISTS sparse_tuple;

CREATE TABLE sparse_tuple (id UInt64, t Tuple(a UInt64, s String))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, ratio_for_sparse_serialization = 0.5;

INSERT INTO sparse_tuple SELECT number, (if (number % 20 = 0, number, 0), repeat('a', number % 10 + 1)) FROM numbers(1000);

SELECT t FROM sparse_tuple ORDER BY id LIMIT 5;
SELECT t FROM sparse_tuple WHERE t.a != 0 ORDER BY id LIMIT 5;
SELECT t FROM sparse_tuple WHERE t.a != 0 ORDER BY t.a LIMIT 5;

SELECT t.a FROM sparse_tuple ORDER BY id LIMIT 5;
SELECT t.a FROM sparse_tuple WHERE t.a != 0 ORDER BY id LIMIT 5;
SELECT t.a FROM sparse_tuple WHERE t.a != 0 ORDER BY t.a LIMIT 5;

SELECT t.s FROM sparse_tuple ORDER BY id LIMIT 5;
SELECT t.s FROM sparse_tuple WHERE t.a != 0 ORDER BY id LIMIT 5;


DROP TABLE IF EXISTS sparse_tuple;
