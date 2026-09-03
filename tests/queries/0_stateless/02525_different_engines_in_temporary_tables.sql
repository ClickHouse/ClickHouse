DROP TEMPORARY TABLE IF EXISTS table_merge_tree_02525;
CREATE TEMPORARY TABLE table_merge_tree_02525
(
    id UInt64,
    info String
)
ENGINE = MergeTree
ORDER BY id
PRIMARY KEY id;
INSERT INTO table_merge_tree_02525 VALUES (1, 'a'), (2, 'b');
INSERT INTO table_merge_tree_02525 VALUES (3, 'c');
OPTIMIZE TABLE table_merge_tree_02525 FINAL;
SELECT * FROM table_merge_tree_02525;
-- Check that temporary table with MergeTree is not sent to remote servers
-- The query with remote() should not fail
SELECT dummy FROM remote('127.0.0.{1,2}', system, one);
DROP TEMPORARY TABLE table_merge_tree_02525;

DROP TEMPORARY TABLE IF EXISTS table_log_02525;
CREATE TEMPORARY TABLE table_log_02525
(
    id UInt64,
    info String
)
ENGINE = Log;
INSERT INTO table_log_02525 VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT * FROM table_log_02525;
DROP TEMPORARY TABLE table_log_02525;

DROP TEMPORARY TABLE IF EXISTS table_stripe_log_02525;
CREATE TEMPORARY TABLE table_stripe_log_02525
(
    id UInt64,
    info String
)
ENGINE = StripeLog;
INSERT INTO table_stripe_log_02525 VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT * FROM table_stripe_log_02525;
DROP TEMPORARY TABLE table_stripe_log_02525;

DROP TEMPORARY TABLE IF EXISTS table_tiny_log_02525;
CREATE TEMPORARY TABLE table_tiny_log_02525
(
    id UInt64,
    info String
)
ENGINE = TinyLog;
INSERT INTO table_tiny_log_02525 VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT * FROM table_tiny_log_02525;
DROP TEMPORARY TABLE table_tiny_log_02525;

DROP TEMPORARY TABLE IF EXISTS table_replicated_merge_tree_02525;
CREATE TEMPORARY TABLE table_replicated_merge_tree_02525
(
    id UInt64,
    info String
)
ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_02525/table_replicated_merge_tree_02525', 'r1')
ORDER BY id
PRIMARY KEY id; -- { serverError INCORRECT_QUERY }

DROP TEMPORARY TABLE IF EXISTS table_keeper_map_02525;
CREATE TEMPORARY TABLE table_keeper_map_02525
(
    key String,
    value UInt32
) Engine=KeeperMap('/' || currentDatabase() || '/test02525')
PRIMARY KEY(key); -- { serverError INCORRECT_QUERY }
