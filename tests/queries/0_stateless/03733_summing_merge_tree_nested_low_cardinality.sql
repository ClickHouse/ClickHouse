CREATE TABLE sums (key LowCardinality(String), sumOfSums UInt64, sumsMap Nested (key LowCardinality(String), sum UInt64)) ENGINE = SummingMergeTree PRIMARY KEY (key);

INSERT INTO sums (key, sumOfSums, sumsMap.key, sumsMap.sum) VALUES ('lol', 3, ['a', 'b'], [1, 2]);
INSERT INTO sums (key, sumOfSums, sumsMap.key, sumsMap.sum) VALUES ('lol', 7, ['a', 'b'], [3, 4]);

OPTIMIZE TABLE sums;

SELECT * FROM sums;
