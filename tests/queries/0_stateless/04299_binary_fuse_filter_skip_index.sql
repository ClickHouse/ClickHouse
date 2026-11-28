SET allow_experimental_binary_fuse_filter_index = 1;

-- { echo }

DROP TABLE IF EXISTS t_bf;
DROP TABLE IF EXISTS t_bloom;

CREATE TABLE t_bf
(
    `k` UInt64,
    INDEX idx_k k TYPE binary_fuse_filter(0.05) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 64;

CREATE TABLE t_bloom
(
    `k` UInt64,
    INDEX idx_k k TYPE bloom_filter(0.05) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 64;

INSERT INTO t_bf SELECT number FROM numbers(200);
INSERT INTO t_bloom SELECT number FROM numbers(200);

SELECT count() FROM t_bf WHERE k = 137;
SELECT count() FROM t_bloom WHERE k = 137;

SELECT count() FROM t_bf WHERE k != 7;
SELECT count() FROM t_bloom WHERE k != 7;

SELECT count() FROM t_bf WHERE k NOT IN (7);
SELECT count() FROM t_bloom WHERE k NOT IN (7);

-- A granule can hold both matching and non-matching rows, so the negated forms are not index-backed.
SELECT count() FROM t_bf WHERE k != 7 SETTINGS force_data_skipping_indices = 'idx_k'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_bf WHERE k NOT IN (7) SETTINGS force_data_skipping_indices = 'idx_k'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_bf WHERE k NOT IN (7, 8) SETTINGS force_data_skipping_indices = 'idx_k'; -- { serverError INDEX_NOT_USED }

-- With `transform_null_in = 1`, `IN` becomes `nullIn`; a NULL-free set of the index type still prunes.
SELECT count() FROM t_bf WHERE k IN (1000000, 1000001) SETTINGS transform_null_in = 1, force_data_skipping_indices = 'idx_k';
SELECT count() FROM t_bf WHERE k IN (7, 1000000) SETTINGS transform_null_in = 1, force_data_skipping_indices = 'idx_k';

SELECT count() FROM t_bf WHERE k IN (SELECT number FROM numbers(200));
SELECT count() FROM t_bloom WHERE k IN (SELECT number FROM numbers(200));

SELECT count() FROM t_bf WHERE k IN (1000000, 1000001, 1000002)
SETTINGS force_data_skipping_indices = 'idx_k';

SELECT (sum(explain LIKE '%Skip%') > 0) AND (sum(explain LIKE '%idx_k%') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT * FROM t_bf WHERE k = 42
);

SELECT (sum(explain LIKE '%Skip%') > 0) AND (sum(explain LIKE '%idx_k%') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT * FROM t_bloom WHERE k = 42
);

DROP TABLE t_bf;
DROP TABLE t_bloom;

DROP TABLE IF EXISTS t_empty_bf;
DROP TABLE IF EXISTS t_empty_b;

CREATE TABLE t_empty_bf
(
    `k` UInt64,
    INDEX idx_k k TYPE binary_fuse_filter(0.05) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 64;

CREATE TABLE t_empty_b
(
    `k` UInt64,
    INDEX idx_k k TYPE bloom_filter(0.05) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 64;

SELECT count() FROM t_empty_bf WHERE k = 0;
SELECT count() FROM t_empty_b WHERE k = 0;

DROP TABLE t_empty_bf;
DROP TABLE t_empty_b;

DROP TABLE IF EXISTS t_str_bf;
DROP TABLE IF EXISTS t_str_b;

CREATE TABLE t_str_bf
(
    `s` String,
    INDEX idx_s s TYPE binary_fuse_filter(0.05) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY s
SETTINGS index_granularity = 64;

CREATE TABLE t_str_b
(
    `s` String,
    INDEX idx_s s TYPE bloom_filter(0.05) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY s
SETTINGS index_granularity = 64;

INSERT INTO t_str_bf SELECT toString(number % 3) FROM numbers(300);
INSERT INTO t_str_b SELECT toString(number % 3) FROM numbers(300);

SELECT count() FROM t_str_bf WHERE s = '1';
SELECT count() FROM t_str_b WHERE s = '1';

SELECT count() FROM t_str_bf WHERE s != '0';
SELECT count() FROM t_str_b WHERE s != '0';

DROP TABLE t_str_bf;
DROP TABLE t_str_b;
