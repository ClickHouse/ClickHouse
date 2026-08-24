DROP TABLE IF EXISTS lc_00931;

CREATE TABLE lc_00931 (
    key UInt64,
    value Array(LowCardinality(String)))
ENGINE = MergeTree
ORDER BY key;

INSERT INTO lc_00931 SELECT number,
if (number < 10000 OR number > 100000,
    [toString(number)],
    emptyArrayString())
    FROM system.numbers LIMIT 200000;

SELECT * FROM lc_00931
WHERE (key < 100 OR key > 50000)
    AND NOT has(value, toString(key))
    AND length(value) == 1
LIMIT 10
SETTINGS max_block_size = 8192,
         max_threads = 1;

DROP TABLE IF EXISTS lc_00931;
