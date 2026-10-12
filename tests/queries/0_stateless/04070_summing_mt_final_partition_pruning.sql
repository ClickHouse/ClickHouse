
DROP TABLE IF EXISTS tab_summing_part;

CREATE TABLE tab_summing_part (id UInt32, category String, value UInt32)
ENGINE = SummingMergeTree() PARTITION BY category ORDER BY id;

SYSTEM STOP MERGES tab_summing_part;

INSERT INTO tab_summing_part VALUES (1, 'a', 10);
INSERT INTO tab_summing_part VALUES (1, 'b', 20);
INSERT INTO tab_summing_part VALUES (2, 'c', 50);

SELECT '--- SummingMergeTree FINAL no filter';
SELECT id, value FROM tab_summing_part FINAL ORDER BY id;

SELECT '--- SummingMergeTree FINAL WHERE category != b';
SELECT id, value FROM tab_summing_part FINAL WHERE category != 'b' ORDER BY id;

SELECT '--- SummingMergeTree FINAL WHERE category = a';
SELECT id, value FROM tab_summing_part FINAL WHERE category = 'a' ORDER BY id;

DROP TABLE tab_summing_part;

DROP TABLE IF EXISTS tab_agg_part;

CREATE TABLE tab_agg_part (id UInt32, category String, cnt SimpleAggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree() PARTITION BY category ORDER BY id;

SYSTEM STOP MERGES tab_agg_part;

INSERT INTO tab_agg_part VALUES (1, 'x', 5);
INSERT INTO tab_agg_part VALUES (1, 'y', 15);

SELECT '--- AggregatingMergeTree FINAL no filter';
SELECT id, cnt FROM tab_agg_part FINAL ORDER BY id;

SELECT '--- AggregatingMergeTree FINAL WHERE category != x';
SELECT id, cnt FROM tab_agg_part FINAL WHERE category != 'x' ORDER BY id;

DROP TABLE tab_agg_part;
