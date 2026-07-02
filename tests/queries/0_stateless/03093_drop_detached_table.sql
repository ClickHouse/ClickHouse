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
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'MergeTree DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_merge_tree_perm  (number UInt64) ENGINE=MergeTree ORDER BY number;
INSERT INTO test_db.test_table_03093_merge_tree_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_merge_tree_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_merge_tree_perm SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'ReplacingMergeTree';
CREATE TABLE test_db.test_table_03093_repl_merge_tree  (number UInt64) ENGINE=ReplacingMergeTree() ORDER BY number;
INSERT INTO test_db.test_table_03093_repl_merge_tree SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_repl_merge_tree;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_repl_merge_tree SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'ReplacingMergeTree DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_repl_merge_tree_perm  (number UInt64) ENGINE=ReplacingMergeTree() ORDER BY number;
INSERT INTO test_db.test_table_03093_repl_merge_tree_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_repl_merge_tree_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_repl_merge_tree_perm SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'SummingMergeTree';
CREATE TABLE test_db.test_table_03093_sum_merge_tree  (number UInt64) ENGINE=SummingMergeTree ORDER BY number;
INSERT INTO test_db.test_table_03093_sum_merge_tree SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_sum_merge_tree;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_sum_merge_tree SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'SummingMergeTree DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_sum_merge_tree_perm  (number UInt64) ENGINE=SummingMergeTree ORDER BY number;
INSERT INTO test_db.test_table_03093_sum_merge_tree_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_sum_merge_tree_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_sum_merge_tree_perm SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'AggregatingMergeTree';
CREATE TABLE test_db.test_table_03093_agg_merge_tree  (number UInt64) ENGINE=AggregatingMergeTree ORDER BY number;
INSERT INTO test_db.test_table_03093_agg_merge_tree SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_agg_merge_tree;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_agg_merge_tree SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'AggregatingMergeTree DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_agg_merge_tree_perm  (number UInt64) ENGINE=AggregatingMergeTree ORDER BY number;
INSERT INTO test_db.test_table_03093_agg_merge_tree_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_agg_merge_tree_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_agg_merge_tree_perm SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'CollapsingMergeTree';
CREATE TABLE test_db.test_table_03093_col_merge_tree  (number UInt64, sign Int8) ENGINE=CollapsingMergeTree(sign) ORDER BY number;
INSERT INTO test_db.test_table_03093_col_merge_tree SELECT number, 1 FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_col_merge_tree;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_col_merge_tree SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'CollapsingMergeTree DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_col_merge_tree_perm (number UInt64, sign Int8) ENGINE=CollapsingMergeTree(sign) ORDER BY number;
INSERT INTO test_db.test_table_03093_col_merge_tree_perm SELECT number, 1 FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_col_merge_tree_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_col_merge_tree_perm SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'VersionedCollapsingMergeTree';
CREATE TABLE test_db.test_table_03093_vcol_merge_tree (number UInt64, sign Int8, version Int32) ENGINE=VersionedCollapsingMergeTree(sign, version) ORDER BY number;
INSERT INTO test_db.test_table_03093_vcol_merge_tree SELECT number, 1, 1 FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_vcol_merge_tree;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_vcol_merge_tree SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'VersionedCollapsingMergeTree DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_vcol_merge_tree_perm (number UInt64, sign Int8, version Int32) ENGINE=VersionedCollapsingMergeTree(sign, version) ORDER BY number;
INSERT INTO test_db.test_table_03093_vcol_merge_tree_perm SELECT number, 1, 1 FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_vcol_merge_tree_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_vcol_merge_tree_perm SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'Log';
CREATE TABLE test_db.test_table_03093_log (val Int64, msg String) ENGINE=Log;
INSERT INTO test_db.test_table_03093_log SELECT number, 'some string' FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_log;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_log SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'Log DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_log_perm (val Int64, msg String) ENGINE=Log;
INSERT INTO test_db.test_table_03093_log_perm SELECT number, 'some string' FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_log_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_log_perm SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'TinyLog';
CREATE TABLE test_db.test_table_03093_tiny_log (val Int64, msg String) ENGINE=TinyLog;
INSERT INTO test_db.test_table_03093_tiny_log SELECT number, 'some string' FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_tiny_log;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_tiny_log SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'TinyLog DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_tiny_log_perm (val Int64, msg String) ENGINE=TinyLog;
INSERT INTO test_db.test_table_03093_tiny_log_perm SELECT number, 'some string' FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_tiny_log_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_tiny_log_perm SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'StripeLog';
CREATE TABLE test_db.test_table_03093_stripe_log (val UInt64) Engine=StripeLog();
INSERT INTO test_db.test_table_03093_stripe_log SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_stripe_log;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_stripe_log SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'StripeLog DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_stripe_log_perm (val UInt64) Engine=StripeLog();
INSERT INTO test_db.test_table_03093_stripe_log_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_stripe_log_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_stripe_log_perm SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'Null';
CREATE TABLE test_db.test_table_03093_null (val UInt64) Engine=Null;
INSERT INTO test_db.test_table_03093_null SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_null;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_null SYNC;
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

SELECT 'Null DETACH PERMANENTLY';
CREATE TABLE test_db.test_table_03093_null_perm (val UInt64) Engine=Null;
INSERT INTO test_db.test_table_03093_null_perm SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_db.test_table_03093_null_perm PERMANENTLY;
SELECT table, is_permanently FROM system.detached_tables WHERE database='test_db' FORMAT TabSeparated;
DROP DETACHED TABLE test_db.test_table_03093_null_perm SYNC;
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
SELECT count(table) FROM system.detached_tables WHERE database='test_db';

USE test_db;

SELECT 'reviewer-temp-shadow-feature-gate';
CREATE TABLE test_table_03093_shadow (number UInt64) ENGINE=MergeTree ORDER BY number;
DETACH TABLE test_table_03093_shadow PERMANENTLY;
CREATE TEMPORARY TABLE test_table_03093_shadow (number UInt64) ENGINE=Memory;
SET allow_experimental_drop_detached_table=0;
DROP DETACHED TABLE test_table_03093_shadow SYNC; -- { serverError SUPPORT_IS_DISABLED }
SELECT count()
FROM system.detached_tables
WHERE database='test_db' AND table='test_table_03093_shadow';
SELECT count() FROM test_table_03093_shadow;
SET allow_experimental_drop_detached_table=1;
DROP DETACHED TABLE test_table_03093_shadow SYNC;
SELECT count()
FROM system.detached_tables
WHERE database='test_db' AND table='test_table_03093_shadow';
SELECT count() FROM test_table_03093_shadow;

SELECT 'reviewer-if-exists-atomic';
DROP DETACHED TABLE IF EXISTS test_table_03093_if_exists_missing SYNC;
CREATE TABLE test_table_03093_if_exists_attached (number UInt64)
ENGINE=MergeTree ORDER BY number;
DROP DETACHED TABLE IF EXISTS
    test_table_03093_if_exists_attached SYNC; -- { serverError UNKNOWN_TABLE }
DETACH TABLE test_table_03093_if_exists_attached;
DROP DETACHED TABLE IF EXISTS test_table_03093_if_exists_attached SYNC;
SELECT count()
FROM system.detached_tables
WHERE database='test_db' AND table='test_table_03093_if_exists_attached';

SELECT 'reviewer-if-exists-unsupported-db';
CREATE DATABASE test_memory_db_03093 ENGINE=Memory;
DROP DETACHED TABLE IF EXISTS test_memory_db_03093.missing SYNC;
CREATE TABLE test_memory_db_03093.existing (number UInt64) ENGINE=Memory;
DROP DETACHED TABLE test_memory_db_03093.existing SYNC; -- { serverError UNKNOWN_TABLE }
DROP DATABASE test_memory_db_03093;

SELECT 'reviewer-non-table-detached-objects';
CREATE TABLE test_table_03093_non_table_source (key UInt64, value String)
ENGINE=MergeTree ORDER BY key;
CREATE MATERIALIZED VIEW test_table_03093_non_table_mv
ENGINE=MergeTree ORDER BY key
AS SELECT key FROM test_table_03093_non_table_source;
DETACH TABLE test_table_03093_non_table_mv;
DROP DETACHED TABLE test_table_03093_non_table_mv SYNC; -- { serverError INCORRECT_QUERY }
ATTACH TABLE test_table_03093_non_table_mv;
CREATE DICTIONARY test_table_03093_non_table_dict (key UInt64, value String)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'test_table_03093_non_table_source' DB 'test_db'))
LAYOUT(FLAT()) LIFETIME(0);
DETACH DICTIONARY test_table_03093_non_table_dict;
DROP DETACHED TABLE test_table_03093_non_table_dict SYNC; -- { serverError INCORRECT_QUERY }
ATTACH DICTIONARY test_table_03093_non_table_dict;
DROP DICTIONARY test_table_03093_non_table_dict;
DROP TABLE test_table_03093_non_table_mv;
DROP TABLE test_table_03093_non_table_source;

SELECT 'syntax';
CREATE TABLE test_table_03093_syntax (number UInt64) ENGINE=MergeTree ORDER BY number;
DETACH TABLE test_table_03093_syntax;
DETACH DETACHED TABLE test_table_03093_syntax; -- { clientError SYNTAX_ERROR }
TRUNCATE DETACHED TABLE test_table_03093_syntax; -- { clientError SYNTAX_ERROR }
DROP DETACHED TABLE IF EMPTY test_table_03093_syntax; -- { clientError SYNTAX_ERROR }
DROP DETACHED VIEW test_table_03093_syntax; -- { clientError SYNTAX_ERROR }
DROP DETACHED DICTIONARY test_table_03093_syntax; -- { clientError SYNTAX_ERROR }
DROP DETACHED TABLE TEMPORARY test_table_03093_syntax; -- { clientError SYNTAX_ERROR }
DROP DETACHED TABLE test_table_03093_syntax SYNC;

USE default;

SELECT 'total';
SELECT table FROM system.detached_tables WHERE database='test_db';

DROP DATABASE test_db;
