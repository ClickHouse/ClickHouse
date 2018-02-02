DROP TABLE IF EXISTS test.merge_tree;
DROP TABLE IF EXISTS test.collapsing_merge_tree;
DROP TABLE IF EXISTS test.versioned_collapsing_merge_tree;
DROP TABLE IF EXISTS test.summing_merge_tree;
DROP TABLE IF EXISTS test.summing_merge_tree_with_list_of_columns_to_sum;
DROP TABLE IF EXISTS test.aggregating_merge_tree;

DROP TABLE IF EXISTS test.merge_tree_with_sampling;
DROP TABLE IF EXISTS test.collapsing_merge_tree_with_sampling;
DROP TABLE IF EXISTS test.versioned_collapsing_merge_tree_with_sampling;
DROP TABLE IF EXISTS test.summing_merge_tree_with_sampling;
DROP TABLE IF EXISTS test.summing_merge_tree_with_sampling_with_list_of_columns_to_sum;
DROP TABLE IF EXISTS test.aggregating_merge_tree_with_sampling;

DROP TABLE IF EXISTS test.replicated_merge_tree;
DROP TABLE IF EXISTS test.replicated_collapsing_merge_tree;
DROP TABLE IF EXISTS test.replicated_versioned_collapsing_merge_tree;
DROP TABLE IF EXISTS test.replicated_summing_merge_tree;
DROP TABLE IF EXISTS test.replicated_summing_merge_tree_with_list_of_columns_to_sum;
DROP TABLE IF EXISTS test.replicated_aggregating_merge_tree;

DROP TABLE IF EXISTS test.replicated_merge_tree_with_sampling;
DROP TABLE IF EXISTS test.replicated_collapsing_merge_tree_with_sampling;
DROP TABLE IF EXISTS test.replicated_versioned_collapsing_merge_tree_with_sampling;
DROP TABLE IF EXISTS test.replicated_summing_merge_tree_with_sampling;
DROP TABLE IF EXISTS test.replicated_summing_merge_tree_with_sampling_with_list_of_columns_to_sum;
DROP TABLE IF EXISTS test.replicated_aggregating_merge_tree_with_sampling;


CREATE TABLE test.merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = MergeTree(d, (a, b), 111);
CREATE TABLE test.collapsing_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = CollapsingMergeTree(d, (a, b), 111, y);
CREATE TABLE test.versioned_collapsing_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = VersionedCollapsingMergeTree(d, (a, b), 111, y, b);
CREATE TABLE test.summing_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = SummingMergeTree(d, (a, b), 111);
CREATE TABLE test.summing_merge_tree_with_list_of_columns_to_sum
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = SummingMergeTree(d, (a, b), 111, (y, z));
CREATE TABLE test.aggregating_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = AggregatingMergeTree(d, (a, b), 111);

CREATE TABLE test.merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = MergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);
CREATE TABLE test.collapsing_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = CollapsingMergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b), 111, y);
CREATE TABLE test.versioned_collapsing_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = VersionedCollapsingMergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b, b), 111, y, b);
CREATE TABLE test.summing_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = SummingMergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);
CREATE TABLE test.summing_merge_tree_with_sampling_with_list_of_columns_to_sum
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = SummingMergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b), 111, (y, z));
CREATE TABLE test.aggregating_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = AggregatingMergeTree(d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);

CREATE TABLE test.replicated_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/01/replicated_merge_tree/', 'r1', d, (a, b), 111);
CREATE TABLE test.replicated_collapsing_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/test/01/replicated_collapsing_merge_tree/', 'r1', d, (a, b), 111, y);
CREATE TABLE test.replicated_versioned_collapsing_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/test/01/replicated_versioned_collapsing_merge_tree/', 'r1', d, (a, b), 111, y, b);
CREATE TABLE test.replicated_summing_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/test/01/replicated_summing_merge_tree/', 'r1', d, (a, b), 111);
CREATE TABLE test.replicated_summing_merge_tree_with_list_of_columns_to_sum
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/test/01/replicated_summing_merge_tree_with_list_of_columns_to_sum/', 'r1', d, (a, b), 111, (y, z));
CREATE TABLE test.replicated_aggregating_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/test/01/replicated_aggregating_merge_tree/', 'r1', d, (a, b), 111);

CREATE TABLE test.replicated_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/01/replicated_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);
CREATE TABLE test.replicated_collapsing_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/test/01/replicated_collapsing_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111, y);
CREATE TABLE test.replicated_versioned_collapsing_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/test/01/replicated_versioned_collapsing_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b, b), 111, y, b);
CREATE TABLE test.replicated_summing_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/test/01/replicated_summing_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);
CREATE TABLE test.replicated_summing_merge_tree_with_sampling_with_list_of_columns_to_sum
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/test/01/replicated_summing_merge_tree_with_sampling_with_list_of_columns_to_sum/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111, (y, z));
CREATE TABLE test.replicated_aggregating_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/test/01/replicated_aggregating_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);


INSERT INTO test.merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.collapsing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.versioned_collapsing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.summing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.summing_merge_tree_with_list_of_columns_to_sum VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.aggregating_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);

INSERT INTO test.merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.collapsing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.versioned_collapsing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.summing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.summing_merge_tree_with_sampling_with_list_of_columns_to_sum VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.aggregating_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);

INSERT INTO test.replicated_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.replicated_collapsing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.replicated_versioned_collapsing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.replicated_summing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.replicated_summing_merge_tree_with_list_of_columns_to_sum VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.replicated_aggregating_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);

INSERT INTO test.replicated_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.replicated_collapsing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.replicated_versioned_collapsing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.replicated_summing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.replicated_summing_merge_tree_with_sampling_with_list_of_columns_to_sum VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO test.replicated_aggregating_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);


DROP TABLE test.merge_tree;
DROP TABLE test.collapsing_merge_tree;
DROP TABLE test.versioned_collapsing_merge_tree;
DROP TABLE test.summing_merge_tree;
DROP TABLE test.summing_merge_tree_with_list_of_columns_to_sum;
DROP TABLE test.aggregating_merge_tree;

DROP TABLE test.merge_tree_with_sampling;
DROP TABLE test.collapsing_merge_tree_with_sampling;
DROP TABLE test.versioned_collapsing_merge_tree_with_sampling;
DROP TABLE test.summing_merge_tree_with_sampling;
DROP TABLE test.summing_merge_tree_with_sampling_with_list_of_columns_to_sum;
DROP TABLE test.aggregating_merge_tree_with_sampling;

DROP TABLE test.replicated_merge_tree;
DROP TABLE test.replicated_collapsing_merge_tree;
DROP TABLE test.replicated_versioned_collapsing_merge_tree;
DROP TABLE test.replicated_summing_merge_tree;
DROP TABLE test.replicated_summing_merge_tree_with_list_of_columns_to_sum;
DROP TABLE test.replicated_aggregating_merge_tree;

DROP TABLE test.replicated_merge_tree_with_sampling;
DROP TABLE test.replicated_collapsing_merge_tree_with_sampling;
DROP TABLE test.replicated_versioned_collapsing_merge_tree_with_sampling;
DROP TABLE test.replicated_summing_merge_tree_with_sampling;
DROP TABLE test.replicated_summing_merge_tree_with_sampling_with_list_of_columns_to_sum;
DROP TABLE test.replicated_aggregating_merge_tree_with_sampling;
