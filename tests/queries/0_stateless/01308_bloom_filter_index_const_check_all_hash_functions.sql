DROP TABLE IF EXISTS test_01308;
CREATE TABLE test_01308 (id Int, i8 Int8, INDEX i8_idx i8 TYPE bloom_filter() GRANULARITY 1) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 6;

INSERT INTO test_01308 (id, i8) select number as id, toInt8(number) from numbers(100) where number != 10;
-- due to order of rows there are collisions, and two granulas should be read
SELECT count() FROM test_01308 WHERE i8 = 1 SETTINGS max_rows_to_read = 6; -- { serverError 158; }

TRUNCATE TABLE test_01308;
INSERT INTO test_01308 (id, i8) select number as id, toInt8(number) from numbers(100);
-- but in this case only one (since the order allows avoid collisions).
SELECT count() FROM test_01308 WHERE i8 = 1 SETTINGS max_rows_to_read = 6;
