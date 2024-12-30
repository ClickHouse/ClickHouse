-- Tags: long, zookeeper, no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database: Old syntax is not allowed
-- Tag no-parallel: leftovers
-- no-shared-merge-tree implemented another test

SET optimize_on_insert = 0;

DROP TABLE IF EXISTS merge_tree;
DROP TABLE IF EXISTS collapsing_merge_tree;
DROP TABLE IF EXISTS versioned_collapsing_merge_tree;
DROP TABLE IF EXISTS summing_merge_tree;
DROP TABLE IF EXISTS summing_merge_tree_with_list_of_columns_to_sum;
DROP TABLE IF EXISTS aggregating_merge_tree;

DROP TABLE IF EXISTS merge_tree_with_sampling;
DROP TABLE IF EXISTS collapsing_merge_tree_with_sampling;
DROP TABLE IF EXISTS versioned_collapsing_merge_tree_with_sampling;
DROP TABLE IF EXISTS summing_merge_tree_with_sampling;
DROP TABLE IF EXISTS summing_merge_tree_with_sampling_with_list_of_columns_to_sum;
DROP TABLE IF EXISTS aggregating_merge_tree_with_sampling;

DROP TABLE IF EXISTS replicated_merge_tree;
DROP TABLE IF EXISTS replicated_collapsing_merge_tree;
DROP TABLE IF EXISTS replicated_versioned_collapsing_merge_tree;
DROP TABLE IF EXISTS replicated_summing_merge_tree;
DROP TABLE IF EXISTS replicated_summing_merge_tree_with_list_of_columns_to_sum;
DROP TABLE IF EXISTS replicated_aggregating_merge_tree;

DROP TABLE IF EXISTS replicated_merge_tree_with_sampling;
DROP TABLE IF EXISTS replicated_collapsing_merge_tree_with_sampling;
DROP TABLE IF EXISTS replicated_versioned_collapsing_merge_tree_with_sampling;
DROP TABLE IF EXISTS replicated_summing_merge_tree_with_sampling;
DROP TABLE IF EXISTS replicated_summing_merge_tree_with_sampling_with_list_of_columns_to_sum;
DROP TABLE IF EXISTS replicated_aggregating_merge_tree_with_sampling;


set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = MergeTree(d, (a, b), 111);
CREATE TABLE collapsing_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = CollapsingMergeTree(d, (a, b), 111, y);
CREATE TABLE versioned_collapsing_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = VersionedCollapsingMergeTree(d, (a, b), 111, y, b);
CREATE TABLE summing_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = SummingMergeTree(d, (a, b), 111);
CREATE TABLE summing_merge_tree_with_list_of_columns_to_sum
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = SummingMergeTree(d, (a, b), 111, (y, z));
CREATE TABLE aggregating_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = AggregatingMergeTree(d, (a, b), 111);

CREATE TABLE merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = MergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);
CREATE TABLE collapsing_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = CollapsingMergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b), 111, y);
CREATE TABLE versioned_collapsing_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = VersionedCollapsingMergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b, b), 111, y, b);
CREATE TABLE summing_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = SummingMergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);
CREATE TABLE summing_merge_tree_with_sampling_with_list_of_columns_to_sum
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = SummingMergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b), 111, (y, z));
CREATE TABLE aggregating_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = AggregatingMergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);

CREATE TABLE replicated_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00083/01/replicated_merge_tree/', 'r1', d, (a, b), 111);
CREATE TABLE replicated_collapsing_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/{database}/test_00083/01/replicated_collapsing_merge_tree/', 'r1', d, (a, b), 111, y);
CREATE TABLE replicated_versioned_collapsing_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/{database}/test_00083/01/replicated_versioned_collapsing_merge_tree/', 'r1', d, (a, b), 111, y, b);
CREATE TABLE replicated_summing_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{database}/test_00083/01/replicated_summing_merge_tree/', 'r1', d, (a, b), 111);
CREATE TABLE replicated_summing_merge_tree_with_list_of_columns_to_sum
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{database}/test_00083/01/replicated_summing_merge_tree_with_list_of_columns_to_sum/', 'r1', d, (a, b), 111, (y, z));
CREATE TABLE replicated_aggregating_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{database}/test_00083/01/replicated_aggregating_merge_tree/', 'r1', d, (a, b), 111);

CREATE TABLE replicated_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00083/01/replicated_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);
CREATE TABLE replicated_collapsing_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/{database}/test_00083/01/replicated_collapsing_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111, y);
CREATE TABLE replicated_versioned_collapsing_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/{database}/test_00083/01/replicated_versioned_collapsing_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b, b), 111, y, b);
CREATE TABLE replicated_summing_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{database}/test_00083/01/replicated_summing_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);
CREATE TABLE replicated_summing_merge_tree_with_sampling_with_list_of_columns_to_sum
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{database}/test_00083/01/replicated_summing_merge_tree_with_sampling_with_list_of_columns_to_sum/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111, (y, z));
CREATE TABLE replicated_aggregating_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{database}/test_00083/01/replicated_aggregating_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);


INSERT INTO merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO collapsing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO versioned_collapsing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO summing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO summing_merge_tree_with_list_of_columns_to_sum VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO aggregating_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);

INSERT INTO merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO collapsing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO versioned_collapsing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO summing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO summing_merge_tree_with_sampling_with_list_of_columns_to_sum VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO aggregating_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);

INSERT INTO replicated_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_collapsing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_versioned_collapsing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_summing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_summing_merge_tree_with_list_of_columns_to_sum VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_aggregating_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);

INSERT INTO replicated_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_collapsing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_versioned_collapsing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_summing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_summing_merge_tree_with_sampling_with_list_of_columns_to_sum VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO replicated_aggregating_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);


DROP TABLE merge_tree;
DROP TABLE collapsing_merge_tree;
DROP TABLE versioned_collapsing_merge_tree;
DROP TABLE summing_merge_tree;
DROP TABLE summing_merge_tree_with_list_of_columns_to_sum;
DROP TABLE aggregating_merge_tree;

DROP TABLE merge_tree_with_sampling;
DROP TABLE collapsing_merge_tree_with_sampling;
DROP TABLE versioned_collapsing_merge_tree_with_sampling;
DROP TABLE summing_merge_tree_with_sampling;
DROP TABLE summing_merge_tree_with_sampling_with_list_of_columns_to_sum;
DROP TABLE aggregating_merge_tree_with_sampling;

DROP TABLE replicated_merge_tree;
DROP TABLE replicated_collapsing_merge_tree;
DROP TABLE replicated_versioned_collapsing_merge_tree;
DROP TABLE replicated_summing_merge_tree;
DROP TABLE replicated_summing_merge_tree_with_list_of_columns_to_sum;
DROP TABLE replicated_aggregating_merge_tree;

DROP TABLE replicated_merge_tree_with_sampling;
DROP TABLE replicated_collapsing_merge_tree_with_sampling;
DROP TABLE replicated_versioned_collapsing_merge_tree_with_sampling;
DROP TABLE replicated_summing_merge_tree_with_sampling;
DROP TABLE replicated_summing_merge_tree_with_sampling_with_list_of_columns_to_sum;
DROP TABLE replicated_aggregating_merge_tree_with_sampling;
