DROP TABLE IF EXISTS test2;

CREATE TABLE test2
(
    k UInt64
) ENGINE = MergeTree ORDER BY k SETTINGS min_compress_block_size = 10240, min_bytes_for_wide_part = 1, max_compression_threads = 64;

INSERT INTO test2 SELECT number FROM numbers(20000);
SELECT sum(k) = (9999 * 10000 / 2 + 10000 * 9999) FROM test2 WHERE k > 10000;

DROP TABLE test2;
