-- Test that `_table` virtual column works correctly for Merge over Distributed.
-- Reproduces the m4 case from 03094: Merge(Merge(plain, Distributed), plain).

DROP TABLE IF EXISTS outer_merge;
DROP TABLE IF EXISTS inner_merge;
DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS local_mt;
DROP TABLE IF EXISTS plain_mt1;
DROP TABLE IF EXISTS plain_mt2;

CREATE TABLE plain_mt1 (key Int, value Int) ENGINE = MergeTree ORDER BY key;
CREATE TABLE plain_mt2 (key Int, value Int) ENGINE = MergeTree ORDER BY key;
CREATE TABLE local_mt (key Int, value Int) ENGINE = Memory;
CREATE TABLE dist AS local_mt ENGINE = Distributed(test_shard_localhost, currentDatabase(), local_mt, rand());

INSERT INTO plain_mt1 VALUES (1, 10), (2, 20);
INSERT INTO plain_mt2 VALUES (3, 30), (4, 40);
INSERT INTO local_mt VALUES (5, 50), (6, 60);

-- inner_merge reads from plain_mt1 and dist (-> local_mt)
CREATE TABLE inner_merge AS plain_mt1 ENGINE = Merge(currentDatabase(), '^(plain_mt1|dist)$');
-- outer_merge reads from inner_merge and plain_mt2
CREATE TABLE outer_merge AS plain_mt1 ENGINE = Merge(currentDatabase(), '^(inner_merge|plain_mt2)$');

SELECT 'all', _table, key, value FROM outer_merge ORDER BY key;
SELECT 'filter_local', _table, key, value FROM outer_merge WHERE _table = 'local_mt' ORDER BY key;

DROP TABLE outer_merge;
DROP TABLE inner_merge;
DROP TABLE dist;
DROP TABLE local_mt;
DROP TABLE plain_mt2;
DROP TABLE plain_mt1;
