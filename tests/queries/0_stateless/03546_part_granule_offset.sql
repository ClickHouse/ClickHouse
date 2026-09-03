-- { echo ON }

DROP TABLE IF EXISTS test_part_granule_offset;

CREATE TABLE test_part_granule_offset (n UInt64) ENGINE = MergeTree ORDER BY () SETTINGS index_granularity = 2;

INSERT INTO test_part_granule_offset SELECT number FROM numbers(101);

OPTIMIZE TABLE test_part_granule_offset FINAL;

SELECT _part_granule_offset FROM test_part_granule_offset WHERE n < 10 ORDER BY all;

SELECT _part_granule_offset, groupArraySorted(200)(n) FROM test_part_granule_offset GROUP BY _part_granule_offset ORDER BY ALL;

SELECT * FROM test_part_granule_offset WHERE _part_granule_offset % 10 = 1 ORDER BY ALL;

DROP TABLE test_part_granule_offset;
