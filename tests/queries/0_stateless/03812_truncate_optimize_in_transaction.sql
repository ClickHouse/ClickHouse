-- Tags: no-encrypted-storage
-- Tag: no-encrypted-storage: not support transactions

DROP TABLE IF EXISTS test_table;
DROP TABLE IF EXISTS t2;

CREATE TABLE test_table (key UInt64, value String) ENGINE = MergeTree ORDER BY key SETTINGS merge_tree_clear_old_parts_interval_seconds=30;
CREATE TABLE t2 (key UInt64, value String) ENGINE = Memory;

INSERT INTO TABLE test_table VALUES (1, 'a'), (2, 'b'), (1, 'a');

SET throw_on_unsupported_query_inside_transaction=0;
SET implicit_transaction=1;
OPTIMIZE TABLE test_table FINAL DEDUPLICATE BY key, value PARALLEL WITH  OPTIMIZE TABLE t2 SETTINGS max_threads=10; -- { serverError INVALID_TRANSACTION, NOT_IMPLEMENTED }

SET implicit_transaction=0;
TRUNCATE TABLE test_table;
