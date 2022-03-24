-- Tags: no-backward-compatibility-check:21.9.1.1

CREATE TABLE IF NOT EXISTS sample_incorrect
(`x` UUID)
ENGINE = MergeTree
ORDER BY tuple(x)
SAMPLE BY x;  -- { serverError 59 }

DROP TABLE IF EXISTS sample_correct;
CREATE TABLE IF NOT EXISTS sample_correct
(`x` String)
ENGINE = MergeTree
ORDER BY tuple(sipHash64(x))
SAMPLE BY sipHash64(x);

DROP TABLE sample_correct;
