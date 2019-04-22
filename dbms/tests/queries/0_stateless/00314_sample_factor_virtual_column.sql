DROP TABLE IF EXISTS test.sample_00314_1;
DROP TABLE IF EXISTS test.sample_00314_2;
DROP TABLE IF EXISTS test.sample_merge_00314;

CREATE TABLE test.sample_00314_1 (x UInt64, d Date DEFAULT today()) ENGINE = MergeTree(d, intHash64(x), intHash64(x), 10);
CREATE TABLE test.sample_00314_2 (x UInt64, d Date DEFAULT today()) ENGINE = MergeTree(d, intHash64(x), intHash64(x), 10);

SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

INSERT INTO test.sample_00314_1 (x) SELECT number AS x FROM system.numbers LIMIT 1000000;
INSERT INTO test.sample_00314_2 (x) SELECT number AS x FROM system.numbers LIMIT 2000000;

CREATE TABLE test.sample_merge_00314 AS test.sample_00314_1 ENGINE = Merge(test, '^sample_00314_\\d$');

SELECT abs(sum(_sample_factor) - 3000000) / 3000000 < 0.001 FROM test.sample_merge_00314 SAMPLE 100000;
SELECT abs(sum(_sample_factor) - 3000000) / 3000000 < 0.001 FROM merge(test, '^sample_00314_\\d$') SAMPLE 100000;

DROP TABLE test.sample_00314_1;
DROP TABLE test.sample_00314_2;
DROP TABLE test.sample_merge_00314;
