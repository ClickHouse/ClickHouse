CREATE TABLE tab
(
    `foo` Array(LowCardinality(String)),
    INDEX idx foo TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
PRIMARY KEY tuple();

INSERT INTO tab SELECT if(number % 2, ['value'], [])
FROM system.numbers
LIMIT 10000;

SELECT *
FROM tab
PREWHERE indexHint(indexHint(-1, 0.))
WHERE has(foo, 'b');

SELECT *
FROM tab
PREWHERE indexHint(0);
