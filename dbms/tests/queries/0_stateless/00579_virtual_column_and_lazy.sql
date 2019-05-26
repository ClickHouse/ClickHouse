DROP TABLE IF EXISTS test.sample_00579_1;
DROP TABLE IF EXISTS test.sample_00579_2;
DROP TABLE IF EXISTS test.sample_merge_00579;

CREATE TABLE test.sample_00579_1 (x UInt64, d Date DEFAULT today()) ENGINE = MergeTree(d, intHash64(x), intHash64(x), 10);
CREATE TABLE test.sample_00579_2 (x UInt64, d Date DEFAULT today()) ENGINE = MergeTree(d, intHash64(x), intHash64(x), 10);

INSERT INTO test.sample_00579_1 (x) SELECT number AS x FROM system.numbers LIMIT 1000;
INSERT INTO test.sample_00579_2 (x) SELECT number AS x FROM system.numbers LIMIT 2000;

CREATE TABLE test.sample_merge_00579 AS test.sample_00579_1 ENGINE = Merge(test, '^sample_00579_\\d$');

SET max_threads = 1;
SELECT _sample_factor FROM merge(test, '^sample_00579_\\d$');

DROP TABLE test.sample_00579_1;
DROP TABLE test.sample_00579_2;
DROP TABLE test.sample_merge_00579;
