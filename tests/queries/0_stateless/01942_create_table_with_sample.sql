CREATE TABLE IF NOT EXISTS sample_incorrect
(`x` UUID)
ENGINE = MergeTree
ORDER BY tuple(x)
SAMPLE BY x;  -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

DROP TABLE IF EXISTS sample_correct;
CREATE TABLE IF NOT EXISTS sample_correct
(`x` String)
ENGINE = MergeTree
ORDER BY tuple(sipHash64(x))
SAMPLE BY sipHash64(x);

DROP TABLE sample_correct;
