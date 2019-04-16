DROP TABLE IF EXISTS sample1;
DROP TABLE IF EXISTS sample2;
DROP TABLE IF EXISTS sample_merge;

CREATE TABLE sample1 (x UInt64, d Date DEFAULT today()) ENGINE = MergeTree(d, intHash64(x), intHash64(x), 10);
CREATE TABLE sample2 (x UInt64, d Date DEFAULT today()) ENGINE = MergeTree(d, intHash64(x), intHash64(x), 10);

SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

INSERT INTO sample1 (x) SELECT number AS x FROM system.numbers LIMIT 1000000;
INSERT INTO sample2 (x) SELECT number AS x FROM system.numbers LIMIT 2000000;

CREATE TABLE sample_merge AS sample1 ENGINE = Merge(test, '^sample\\d$');

SELECT abs(sum(_sample_factor) - 3000000) / 3000000 < 0.001 FROM sample_merge SAMPLE 100000;
SELECT abs(sum(_sample_factor) - 3000000) / 3000000 < 0.001 FROM merge(test, '^sample\\d$') SAMPLE 100000;

DROP TABLE sample1;
DROP TABLE sample2;
DROP TABLE sample_merge;
