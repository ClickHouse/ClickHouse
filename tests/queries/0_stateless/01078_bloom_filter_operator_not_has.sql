DROP TABLE IF EXISTS bloom_filter_not_has;

CREATE TABLE bloom_filter_not_has (ary Array(LowCardinality(Nullable(String))), d Date, INDEX idx_ary ary TYPE bloom_filter(0.01) GRANULARITY 1024) ENGINE = MergeTree() PARTITION BY d ORDER BY d;

INSERT INTO bloom_filter_not_has VALUES ([], '2020-02-27') (['o','a'], '2020-02-27') (['e','a','b'], '2020-02-27');
INSERT INTO bloom_filter_not_has VALUES (['o','a','b','c'], '2020-02-27') (['e','a','b','c','d'], '2020-02-27');

SELECT count() FROM bloom_filter_not_has WHERE has(ary, 'a');
SELECT count() FROM bloom_filter_not_has WHERE NOT has(ary, 'a');

SELECT count() FROM bloom_filter_not_has WHERE has(ary, 'b');
SELECT * FROM bloom_filter_not_has WHERE NOT has(ary, 'b') ORDER BY ary;

SELECT count() FROM bloom_filter_not_has WHERE has(ary, 'c');
SELECT * FROM bloom_filter_not_has WHERE NOT has(ary, 'c') ORDER BY ary;

SELECT count() FROM bloom_filter_not_has WHERE has(ary, 'd');
SELECT * FROM bloom_filter_not_has WHERE NOT has(ary, 'd') ORDER BY ary;

SELECT count() FROM bloom_filter_not_has WHERE has(ary, 'f');
SELECT * FROM bloom_filter_not_has WHERE NOT has(ary, 'f') ORDER BY ary;

DROP TABLE IF EXISTS bloom_filter_not_has;
