-- Tags: no-parallel, distributed

SET allow_experimental_drop_detached_table=1;

DROP DATABASE IF EXISTS test_db;
CREATE DATABASE test_db ENGINE=Atomic;

SELECT 'MergeTree';
CREATE TABLE test_db.test_table_03093_merge_tree  (number UInt64) ENGINE=MergeTree ORDER BY number;
INSERT INTO test_db.test_table_03093_merge_tree SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_merge_tree;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_merge_tree SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'MergeTree DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_merge_tree_perm  (number UInt64) ENGINE=MergeTree ORDER BY number;
INSERT INTO test_db.test_table_03093_merge_tree_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_merge_tree_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_merge_tree_perm SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'ReplacingMergeTree';
CREATE TABLE test_db.test_table_03093_repl_merge_tree  (number UInt64) ENGINE=ReplacingMergeTree() ORDER BY number;
INSERT INTO test_db.test_table_03093_repl_merge_tree SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_repl_merge_tree;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_repl_merge_tree SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'ReplacingMergeTree DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_repl_merge_tree_perm  (number UInt64) ENGINE=ReplacingMergeTree() ORDER BY number;
INSERT INTO test_db.test_table_03093_repl_merge_tree_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_repl_merge_tree_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_repl_merge_tree_perm SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'SummingMergeTree';
CREATE TABLE test_db.test_table_03093_sum_merge_tree  (number UInt64) ENGINE=SummingMergeTree ORDER BY number;
INSERT INTO test_db.test_table_03093_sum_merge_tree SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_sum_merge_tree;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_sum_merge_tree SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'SummingMergeTree DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_sum_merge_tree_perm  (number UInt64) ENGINE=SummingMergeTree ORDER BY number;
INSERT INTO test_db.test_table_03093_sum_merge_tree_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_sum_merge_tree_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_sum_merge_tree_perm SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'AggregatingMergeTree';
CREATE TABLE test_db.test_table_03093_agg_merge_tree  (number UInt64) ENGINE=AggregatingMergeTree ORDER BY number;
INSERT INTO test_db.test_table_03093_agg_merge_tree SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_agg_merge_tree;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_agg_merge_tree SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'AggregatingMergeTree DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_agg_merge_tree_perm  (number UInt64) ENGINE=AggregatingMergeTree ORDER BY number;
INSERT INTO test_db.test_table_03093_agg_merge_tree_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_agg_merge_tree_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_agg_merge_tree_perm SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'CollapsingMergeTree';
CREATE TABLE test_db.test_table_03093_col_merge_tree  (number UInt64, sign Int8) ENGINE=CollapsingMergeTree(sign) ORDER BY number;
INSERT INTO test_db.test_table_03093_col_merge_tree SELECT number, 1 FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_col_merge_tree;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_col_merge_tree SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'CollapsingMergeTree DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_col_merge_tree_perm (number UInt64, sign Int8) ENGINE=CollapsingMergeTree(sign) ORDER BY number;
INSERT INTO test_db.test_table_03093_col_merge_tree_perm SELECT number, 1 FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_col_merge_tree_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_col_merge_tree_perm SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'VersionedCollapsingMergeTree';
CREATE TABLE test_db.test_table_03093_vcol_merge_tree (number UInt64, sign Int8, version Int32) ENGINE=VersionedCollapsingMergeTree(sign, version) ORDER BY number;
INSERT INTO test_db.test_table_03093_vcol_merge_tree SELECT number, 1, 1 FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_vcol_merge_tree;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_vcol_merge_tree SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'VersionedCollapsingMergeTree DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_vcol_merge_tree_perm (number UInt64, sign Int8, version Int32) ENGINE=VersionedCollapsingMergeTree(sign, version) ORDER BY number;
INSERT INTO test_db.test_table_03093_vcol_merge_tree_perm SELECT number, 1, 1 FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_vcol_merge_tree_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_vcol_merge_tree_perm SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'Log';
CREATE TABLE test_db.test_table_03093_log (val Int64, msg String) ENGINE=Log;
INSERT INTO test_db.test_table_03093_log SELECT number, 'some string' FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_log;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_log SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'Log DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_log_perm (val Int64, msg String) ENGINE=Log;
INSERT INTO test_db.test_table_03093_log_perm SELECT number, 'some string' FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_log_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_log_perm SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'TinyLog';
CREATE TABLE test_db.test_table_03093_tiny_log (val Int64, msg String) ENGINE=TinyLog;
INSERT INTO test_db.test_table_03093_tiny_log SELECT number, 'some string' FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_tiny_log;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_tiny_log SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'TinyLog DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_tiny_log_perm (val Int64, msg String) ENGINE=TinyLog;
INSERT INTO test_db.test_table_03093_tiny_log_perm SELECT number, 'some string' FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_tiny_log_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_tiny_log_perm SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'StripeLog';
CREATE TABLE test_db.test_table_03093_stripe_log (val UInt64) Engine=StripeLog();
INSERT INTO test_db.test_table_03093_stripe_log SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_stripe_log;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_stripe_log SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'StripeLog DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_stripe_log_perm (val UInt64) Engine=StripeLog();
INSERT INTO test_db.test_table_03093_stripe_log_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_stripe_log_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_stripe_log_perm SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'Null';
CREATE TABLE test_db.test_table_03093_null (val UInt64) Engine=Null;
INSERT INTO test_db.test_table_03093_null SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_null;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_null SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'Null DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_null_perm (val UInt64) Engine=Null;
INSERT INTO test_db.test_table_03093_null_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_null_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_null_perm SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'Buffer';
CREATE TABLE test_db.test_table_03093_null_for_buffer (key UInt64) Engine=Null();
CREATE TABLE test_db.test_table_03093_buffer (key UInt64) Engine=Buffer(test_db, test_table_03093_null_for_buffer,
    1,    /* num_layers */
    10e6, /* min_time, placeholder */
    10e6, /* max_time, placeholder */
    0,    /* min_rows   */
    10e6, /* max_rows   */
    0,    /* min_bytes  */
    80e6  /* max_bytes  */
);
INSERT INTO test_db.test_table_03093_buffer SELECT toUInt64(number) FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_buffer;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_buffer SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'Buffer DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_null_for_buffer_perm (key UInt64) Engine=Null();
CREATE TABLE test_db.test_table_03093_buffer_perm (key UInt64) Engine=Buffer(test_db, test_table_03093_null_for_buffer_perm,
    1,    /* num_layers */
    10e6, /* min_time, placeholder */
    10e6, /* max_time, placeholder */
    0,    /* min_rows   */
    10e6, /* max_rows   */
    0,    /* min_bytes  */
    80e6  /* max_bytes  */
);
INSERT INTO test_db.test_table_03093_buffer_perm SELECT toUInt64(number) FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_buffer_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_buffer_perm SYNC;
SELECT sleep(1) FORMAT Null;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'total';
SELECT table FROM system.detached_tables WHERE database='test_db';

DROP DATABASE test_db;