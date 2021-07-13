CREATE DATABASE IF NOT EXISTS test_sample;

CREATE TABLE IF NOT EXISTS test_sample.sample_incorrect
(`x` UUID)
ENGINE = MergeTree
ORDER BY tuple(x)
SAMPLE BY x;  -- { serverError 59 }

DROP TABLE IF EXISTS test_sample.sample_correct;
CREATE TABLE IF NOT EXISTS test_sample.sample_correct
(`x` String)
ENGINE = MergeTree
ORDER BY tuple(sipHash64(x))
SAMPLE BY sipHash64(x);

DROP TABLE test_sample.sample_correct;

DROP DATABASE test_sample;
