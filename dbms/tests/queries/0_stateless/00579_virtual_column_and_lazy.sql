DROP TABLE IF EXISTS test.sample1;
DROP TABLE IF EXISTS test.sample2;
DROP TABLE IF EXISTS test.sample_merge;

CREATE TABLE test.sample1 (x UInt64, d Date DEFAULT today()) ENGINE = MergeTree(d, intHash64(x), intHash64(x), 10);
CREATE TABLE test.sample2 (x UInt64, d Date DEFAULT today()) ENGINE = MergeTree(d, intHash64(x), intHash64(x), 10);

INSERT INTO test.sample1 (x) SELECT number AS x FROM system.numbers LIMIT 1000;
INSERT INTO test.sample2 (x) SELECT number AS x FROM system.numbers LIMIT 2000;

CREATE TABLE test.sample_merge AS test.sample1 ENGINE = Merge(test, '^sample\\d$');

SET max_threads = 1;
SELECT _sample_factor FROM merge(test, '^sample\\d$');

DROP TABLE test.sample1;
DROP TABLE test.sample2;
DROP TABLE test.sample_merge;
