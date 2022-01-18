SELECT 'Replacing Merge Tree';
DROP TABLE IF EXISTS replacing_merge_tree;
CREATE TABLE replacing_merge_tree (key UInt32, date Datetime) ENGINE=ReplacingMergeTree() PARTITION BY date ORDER BY key;
INSERT INTO replacing_merge_tree VALUES (1, '2020-01-01'), (2, '2020-01-02'), (1, '2020-01-01'), (2, '2020-01-02');
SELECT * FROM replacing_merge_tree ORDER BY key;
DROP TABLE replacing_merge_tree;

SELECT 'Collapsing Merge Tree';
DROP TABLE IF EXISTS collapsing_merge_tree;
CREATE TABLE collapsing_merge_tree (key UInt32, sign Int8, date Datetime) ENGINE=CollapsingMergeTree(sign) PARTITION BY date ORDER BY key;
INSERT INTO collapsing_merge_tree VALUES (1, 1, '2020-01-01'), (2, 1, '2020-01-02'), (1, -1, '2020-01-01'), (2, -1, '2020-01-02'), (1, 1, '2020-01-01');
SELECT * FROM collapsing_merge_tree ORDER BY key;
DROP TABLE collapsing_merge_tree;

SELECT 'Versioned Collapsing Merge Tree';
DROP TABLE IF EXISTS versioned_collapsing_merge_tree;
CREATE TABLE versioned_collapsing_merge_tree (key UInt32, sign Int8, version Int32, date Datetime) ENGINE=VersionedCollapsingMergeTree(sign, version) PARTITION BY date ORDER BY (key, version);
INSERT INTO versioned_collapsing_merge_tree VALUES (1, 1, 1, '2020-01-01'), (1, -1, 1, '2020-01-01'), (1, 1, 2, '2020-01-01');
SELECT * FROM versioned_collapsing_merge_tree ORDER BY key;
DROP TABLE versioned_collapsing_merge_tree;

SELECT 'Summing Merge Tree';
DROP TABLE IF EXISTS summing_merge_tree;
CREATE TABLE summing_merge_tree (key UInt32, val UInt32, date Datetime) ENGINE=SummingMergeTree(val) PARTITION BY date ORDER BY key;
INSERT INTO summing_merge_tree VALUES (1, 1, '2020-01-01'), (2, 1, '2020-01-02'), (1, 5, '2020-01-01'), (2, 5, '2020-01-02');
SELECT * FROM summing_merge_tree ORDER BY key;
DROP TABLE summing_merge_tree;

SELECT 'Aggregating Merge Tree';
DROP TABLE IF EXISTS aggregating_merge_tree;
CREATE TABLE aggregating_merge_tree (key UInt32, val SimpleAggregateFunction(max, UInt32), date Datetime) ENGINE=AggregatingMergeTree() PARTITION BY date ORDER BY key;
INSERT INTO aggregating_merge_tree VALUES (1, 1, '2020-01-01'), (2, 1, '2020-01-02'), (1, 5, '2020-01-01'), (2, 5, '2020-01-02');
SELECT * FROM aggregating_merge_tree ORDER BY key;
DROP TABLE aggregating_merge_tree;

SELECT 'Check creating empty parts';
DROP TABLE IF EXISTS empty;
CREATE TABLE empty (key UInt32, val UInt32, date Datetime) ENGINE=SummingMergeTree(val) PARTITION BY date ORDER BY key;
INSERT INTO empty VALUES (1, 1, '2020-01-01'), (1, 1, '2020-01-01'), (1, -2, '2020-01-01');
SELECT * FROM empty ORDER BY key;
SELECT table, partition, active FROM system.parts where table = 'empty' and active = 1 and database = currentDatabase();
DROP TABLE empty;
