-- Tags: no-parallel, distributed

SET allow_drop_detached_table=1;

CREATE TABLE test_table_03093_merge_tree  (number UInt64) ENGINE=MergeTree ORDER BY number;
INSERT INTO test_table_03093_merge_tree SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_merge_tree;
DROP TABLE test_table_03093_merge_tree SYNC;

CREATE TABLE test_table_03093_merge_tree_perm  (number UInt64) ENGINE=MergeTree ORDER BY number;
INSERT INTO test_table_03093_merge_tree_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_merge_tree_perm PERMANENTLY;
DROP TABLE test_table_03093_merge_tree_perm SYNC;

CREATE TABLE test_table_03093_repl_merge_tree_perm  (number UInt64) ENGINE=ReplacingMergeTree() ORDER BY number;
INSERT INTO test_table_03093_repl_merge_tree_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_repl_merge_tree_perm PERMANENTLY;
DROP TABLE test_table_03093_repl_merge_tree_perm SYNC;

CREATE TABLE test_table_03093_repl_merge_tree  (number UInt64) ENGINE=ReplacingMergeTree() ORDER BY number;
INSERT INTO test_table_03093_repl_merge_tree SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_repl_merge_tree;
DROP TABLE test_table_03093_repl_merge_tree SYNC;

CREATE TABLE test_table_03093_sum_merge_tree_perm  (number UInt64) ENGINE=SummingMergeTree ORDER BY number;
INSERT INTO test_table_03093_sum_merge_tree_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_sum_merge_tree_perm PERMANENTLY;
DROP TABLE test_table_03093_sum_merge_tree_perm SYNC;

CREATE TABLE test_table_03093_sum_merge_tree  (number UInt64) ENGINE=SummingMergeTree ORDER BY number;
INSERT INTO test_table_03093_sum_merge_tree SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_sum_merge_tree;
DROP TABLE test_table_03093_sum_merge_tree SYNC;

CREATE TABLE test_table_03093_agg_merge_tree_perm  (number UInt64) ENGINE=AggregatingMergeTree ORDER BY number;
INSERT INTO test_table_03093_agg_merge_tree_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_agg_merge_tree_perm PERMANENTLY;
DROP TABLE test_table_03093_agg_merge_tree_perm SYNC;

CREATE TABLE test_table_03093_agg_merge_tree  (number UInt64) ENGINE=AggregatingMergeTree ORDER BY number;
INSERT INTO test_table_03093_agg_merge_tree SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_agg_merge_tree;
DROP TABLE test_table_03093_agg_merge_tree SYNC;

CREATE TABLE test_table_03093_col_merge_tree_perm (number UInt64, sign Int8) ENGINE=CollapsingMergeTree(sign) ORDER BY number;
INSERT INTO test_table_03093_col_merge_tree_perm SELECT number, 1 FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_col_merge_tree_perm PERMANENTLY;
DROP TABLE test_table_03093_col_merge_tree_perm SYNC;

CREATE TABLE test_table_03093_col_merge_tree  (number UInt64, sign Int8) ENGINE=CollapsingMergeTree(sign) ORDER BY number;
INSERT INTO test_table_03093_col_merge_tree SELECT number, 1 FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_col_merge_tree;
DROP TABLE test_table_03093_col_merge_tree SYNC;

CREATE TABLE test_table_03093_vcol_merge_tree_perm (number UInt64, sign Int8, version Int32) ENGINE=VersionedCollapsingMergeTree(sign, version) ORDER BY number;
INSERT INTO test_table_03093_vcol_merge_tree_perm SELECT number, 1, 1 FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_vcol_merge_tree_perm PERMANENTLY;
DROP TABLE test_table_03093_vcol_merge_tree_perm SYNC;

CREATE TABLE test_table_03093_vcol_merge_tree (number UInt64, sign Int8, version Int32) ENGINE=VersionedCollapsingMergeTree(sign, version) ORDER BY number;
INSERT INTO test_table_03093_vcol_merge_tree SELECT number, 1, 1 FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_vcol_merge_tree;
DROP TABLE test_table_03093_vcol_merge_tree SYNC;

CREATE TABLE test_table_03093_log (val Int64, msg String) ENGINE=Log;
INSERT INTO test_table_03093_log SELECT number, 'some string' FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_log;
DROP TABLE test_table_03093_log SYNC;

CREATE TABLE test_table_03093_log_perm (val Int64, msg String) ENGINE=Log;
INSERT INTO test_table_03093_log_perm SELECT number, 'some string' FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_log_perm PERMANENTLY;
DROP TABLE test_table_03093_log_perm SYNC;

CREATE TABLE test_table_03093_tiny_log (val Int64, msg String) ENGINE=TinyLog;
INSERT INTO test_table_03093_tiny_log SELECT number, 'some string' FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_tiny_log;
DROP TABLE test_table_03093_tiny_log SYNC;

CREATE TABLE test_table_03093_tiny_log_perm (val Int64, msg String) ENGINE=TinyLog;
INSERT INTO test_table_03093_tiny_log_perm SELECT number, 'some string' FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_tiny_log_perm PERMANENTLY;
DROP TABLE test_table_03093_tiny_log_perm SYNC;

CREATE TABLE test_table_03093_stripe_log (val UInt64) Engine=StripeLog();
INSERT INTO test_table_03093_stripe_log SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_stripe_log;
DROP TABLE test_table_03093_stripe_log SYNC;

CREATE TABLE test_table_03093_stripe_log_perm (val UInt64) Engine=StripeLog();
INSERT INTO test_table_03093_stripe_log_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_stripe_log_perm PERMANENTLY;
DROP TABLE test_table_03093_stripe_log_perm SYNC;

CREATE TABLE test_table_03093_null (val UInt64) Engine=Null;
INSERT INTO test_table_03093_null SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_null;
DROP TABLE test_table_03093_null SYNC;

CREATE TABLE test_table_03093_null_perm (val UInt64) Engine=Null;
INSERT INTO test_table_03093_null_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_null_perm PERMANENTLY;
DROP TABLE test_table_03093_null_perm SYNC;

CREATE TABLE test_table_03093_null_for_buffer (key UInt64) Engine=Null();
CREATE TABLE test_table_03093_buffer_perm (key UInt64) Engine=Buffer(currentDatabase(), test_table_03093_null_for_buffer,
    1,    /* num_layers */
    10e6, /* min_time, placeholder */
    10e6, /* max_time, placeholder */
    0,    /* min_rows   */
    10e6, /* max_rows   */
    0,    /* min_bytes  */
    80e6  /* max_bytes  */
);
INSERT INTO test_table_03093_buffer_perm SELECT toUInt64(number) FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_buffer_perm PERMANENTLY;
DROP TABLE test_table_03093_buffer_perm SYNC;
DROP TABLE test_table_03093_null_for_buffer SYNC;

CREATE TABLE test_table_03093_null_for_buffer (key UInt64) Engine=Null();
CREATE TABLE test_table_03093_buffer (key UInt64) Engine=Buffer(currentDatabase(), test_table_03093_null_for_buffer,
    1,    /* num_layers */
    10e6, /* min_time, placeholder */
    10e6, /* max_time, placeholder */
    0,    /* min_rows   */
    10e6, /* max_rows   */
    0,    /* min_bytes  */
    80e6  /* max_bytes  */
);
INSERT INTO test_table_03093_buffer SELECT toUInt64(number) FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_buffer;
DROP TABLE test_table_03093_buffer SYNC;
DROP TABLE test_table_03093_null_for_buffer SYNC;

CREATE TABLE test_table_03093_for_dist (key Nullable(UInt64)) Engine=TinyLog();
CREATE TABLE test_table_03093_dist (key UInt64) Engine=Distributed('test_cluster_two_shards', currentDatabase(), 'test_table_03093_for_dist', key);
INSERT INTO test_table_03093_dist SELECT toUInt64(number) FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_dist;
DROP TABLE test_table_03093_dist SYNC;
DROP TABLE test_table_03093_for_dist SYNC;

CREATE TABLE test_table_03093_for_dist (key Nullable(UInt64)) Engine=TinyLog();
CREATE TABLE test_table_03093_dist_perm (key UInt64) Engine=Distributed('test_cluster_two_shards', currentDatabase(), 'test_table_03093_for_dist', key);
INSERT INTO test_table_03093_dist_perm SELECT toUInt64(number) FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093_dist_perm PERMANENTLY;
DROP TABLE test_table_03093_dist_perm SYNC;
DROP TABLE test_table_03093_for_dist SYNC;