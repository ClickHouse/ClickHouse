DROP TABLE IF EXISTS cool_table;

CREATE TABLE IF NOT EXISTS cool_table
(
    id UInt64,
    n Nested(n UInt64, lc1 LowCardinality(String))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO cool_table SELECT number, range(number), range(number) FROM numbers(10);

ALTER TABLE cool_table ADD COLUMN IF NOT EXISTS `n.lc2` Array(LowCardinality(String));

SELECT n.lc1, n.lc2 FROM cool_table ORDER BY id;

DROP TABLE IF EXISTS cool_table;

CREATE TABLE IF NOT EXISTS cool_table
(
    id UInt64,
    n Nested(n UInt64, lc1 Array(LowCardinality(String)))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO cool_table SELECT number, range(number), arrayMap(x -> range(x % 4), range(number)) FROM numbers(10);

ALTER TABLE cool_table ADD COLUMN IF NOT EXISTS `n.lc2` Array(Array(LowCardinality(String)));

SELECT n.lc1, n.lc2 FROM cool_table ORDER BY id;

DROP TABLE IF EXISTS cool_table;

CREATE TABLE IF NOT EXISTS cool_table
(
    id UInt64,
    n Nested(n UInt64, lc1 Map(LowCardinality(String), UInt64))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO cool_table SELECT number, range(number), arrayMap(x -> (arrayMap(y -> 'k' || toString(y), range(x % 4)), range(x % 4))::Map(LowCardinality(String), UInt64), range(number)) FROM numbers(10);

ALTER TABLE cool_table ADD COLUMN IF NOT EXISTS `n.lc2` Array(Map(LowCardinality(String), UInt64));

SELECT n.lc1, n.lc2 FROM cool_table ORDER BY id;

DROP TABLE IF EXISTS cool_table;
