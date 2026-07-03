-- Tests for storing the hash table payload of a hash join in row-major form.

DROP TABLE IF EXISTS left;
DROP TABLE IF EXISTS right;
DROP TABLE IF EXISTS right_asof;

CREATE TABLE left (k Int64, t DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE right (k Int64, v1 Nullable(Int64), v2 UInt8, s String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE right_asof (k Int64, t DateTime('UTC'), v2 Nullable(Int64), s String) ENGINE = MergeTree ORDER BY (k, t);

INSERT INTO left SELECT number, toDateTime('2024-01-01 00:00:00', 'UTC') + number FROM numbers(10);
INSERT INTO right SELECT number + 7, number, number, toString(number) FROM numbers(5);
INSERT INTO right VALUES (7, NULL, 5, 'dup');
INSERT INTO right_asof SELECT number, toDateTime('2024-01-01 00:00:00', 'UTC') + number, number, toString(number) FROM numbers(5);

SET join_algorithm = 'hash';
SET min_columns_for_hash_join_row_store = 1;

SELECT '--- INNER JOIN ---';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k ORDER BY ALL;

SELECT '--- LEFT JOIN ---';
SELECT * FROM left l LEFT JOIN right r ON l.k = r.k ORDER BY ALL;

SELECT '--- RIGHT JOIN ---';
SELECT * FROM left l RIGHT JOIN right r ON l.k = r.k ORDER BY ALL;

SELECT '--- FULL JOIN ---';
SELECT * FROM left l FULL JOIN right r ON l.k = r.k ORDER BY ALL;

SELECT '--- ASOF JOIN ---';
SELECT * FROM left l ASOF JOIN right_asof r ON l.k = r.k AND l.t >= r.t ORDER BY ALL;

SELECT '--- Parallel hash JOIN ---';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash';

SELECT '--- Parallel hash FULL JOIN (join_use_nulls) ---';
SELECT * FROM left l FULL JOIN right r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash', join_use_nulls = 1;

SELECT '--- Row-list JOIN output ---';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k ORDER BY ALL SETTINGS join_output_by_rowlist_perkey_rows_threshold = 0;

SELECT '--- Join with block splitting ---';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k ORDER BY ALL SETTINGS max_joined_block_size_rows = 2, joined_block_split_single_row = 1;

DROP TABLE right_asof;
DROP TABLE right;
DROP TABLE left;
