#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database, no-shared-merge-tree

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

# test1: insert with transaction and mutation without transaction
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() PRIMARY KEY tuple();
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 4647429777703185695, 69, 4) LIMIT 86;

    BEGIN TRANSACTION;
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 18218864097310396034, 228, 4) LIMIT 332;
    COMMIT;

    DELETE FROM t0 WHERE TRUE;
"
# DELETE is non-transactional, so its mutation is async at lightweight_deletes_sync=0.
# Wait for it before DETACH SYNC, else ATTACH AS REPLICATED reloads the pre-delete part.
wait_for_all_mutations "t0"
${CLICKHOUSE_CLIENT} -n -q "
    DETACH TABLE t0 SYNC;
    ATTACH TABLE t0 AS REPLICATED;

    SELECT COUNT(*) FROM t0;

    DETACH TABLE t0 SYNC;
    ATTACH TABLE t0 AS NOT REPLICATED;

    DROP TABLE t0;
"

# test2: background merge with transaction and mutation without transaction
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() PRIMARY KEY tuple();
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 4647429777703185695, 69, 4) LIMIT 86;

    BEGIN TRANSACTION;
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 18218864097310396034, 228, 4) LIMIT 332;
    ROLLBACK;

    INSERT INTO TABLE t0 (c0) SELECT CAST(number % 81 AS Int) FROM numbers(350);
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 14142794021619833828, 193, 4) LIMIT 164;
    INSERT INTO TABLE t0 (c0) SELECT 1 FROM numbers(322);
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 12294830401837572975, 254, 4) LIMIT 251;
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 16932182231128798796, 132, 3) LIMIT 98;
    -- Force the merge to complete deterministically before the \`DELETE\` mutation runs.
    -- Without this, an in-flight background merge of the source parts can race with the
    -- mutation cloning the resulting merged part: the merge writes a transient
    -- \`txn_version.txt.tmp\` on the merged part during \`storeInfoToDataPartStorage\`, and on
    -- object storage the mutation's queued hardlink for that \`.tmp\` file is committed later,
    -- after the rename completes, failing with
    -- \`Can't create hardlink for file ... txn_version.txt.tmp (FILE_DOESNT_EXIST)\`.
    -- \`OPTIMIZE FINAL\` is synchronous, so when \`COMMIT\` returns the merged part's metadata
    -- is stable; \`DELETE FROM\` then operates on a single fully-committed part. After that,
    -- only one active part remains, so no further background merges can be scheduled before
    -- the mutation. The merge has to run inside a transaction because the table already had
    -- transactions used on it (\`BEGIN TRANSACTION ... ROLLBACK\` above), and a non-transactional
    -- merge is rejected with \`Cancelling merge ... transactions were enabled for this table\`.
    BEGIN TRANSACTION;
    OPTIMIZE TABLE t0 FINAL;
    COMMIT;
    DELETE FROM t0 WHERE TRUE;
"
# Wait for the non-transactional DELETE mutation before DETACH SYNC (see test1).
wait_for_all_mutations "t0"
${CLICKHOUSE_CLIENT} -n -q "
    DETACH TABLE t0 SYNC;
    ATTACH TABLE t0 AS REPLICATED;

    SELECT COUNT(*) FROM t0;

    DETACH TABLE t0 SYNC;
    ATTACH TABLE t0 AS NOT REPLICATED;

    DROP TABLE t0;
"

# test3: mutation withtransaction after ATTACH TO NOT REPLICATED
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() PRIMARY KEY tuple();
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 4647429777703185695, 69, 4) LIMIT 86;

    BEGIN TRANSACTION;
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 18218864097310396034, 228, 4) LIMIT 332;
    COMMIT;

    DETACH TABLE t0 SYNC;
    ATTACH TABLE t0 AS REPLICATED;

    SELECT COUNT(*) FROM t0;

    DETACH TABLE t0 SYNC;
    ATTACH TABLE t0 AS NOT REPLICATED;

    BEGIN TRANSACTION;
    ALTER TABLE t0 UPDATE c0 = c0 + 1 WHERE 1;
    COMMIT;

    DROP TABLE t0;
"

# test4: a rolled-back merge result intersects a committed mutation of that merge's own source parts.
# The conversion must refuse: the rolled-back part's txn_version.txt is the only record that it is not
# part of the table's data, and without it the part loader cannot resolve the intersection and aborts.
# A rolled-back part is removable as soon as it appears, independently of old_parts_lifetime, so the
# fixture only survives while the parts cleanup does not run: remove_rolled_back_parts_immediately
# turns that off, and the interval settings keep the cleanup task away from the fixture. Explicit
# OPTIMIZE still merges with max_bytes_to_merge_at_max_space_in_pool = 0, while a background merge of
# the two source parts before BEGIN TRANSACTION cannot happen.
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE t4 (c0 Int) ENGINE = MergeTree() PRIMARY KEY tuple()
    SETTINGS old_parts_lifetime = 10000, max_bytes_to_merge_at_max_space_in_pool = 0,
             remove_rolled_back_parts_immediately = 0,
             merge_tree_clear_old_parts_interval_seconds = 100000,
             cleanup_delay_period = 100000, max_cleanup_delay_period = 100000;
    INSERT INTO TABLE t4 (c0) VALUES (1);
    INSERT INTO TABLE t4 (c0) VALUES (2);

    BEGIN TRANSACTION;
    OPTIMIZE TABLE t4 FINAL SETTINGS optimize_throw_if_noop = 1;
    ROLLBACK;

    -- The rolled-back all_1_2_1 is invisible to the mutation, so blocks 1 and 2 are mutated
    -- separately into all_1_1_0_3 and all_2_2_0_3. all_1_2_1 intersects both of them without
    -- containing either, because containment also requires a mutation version at least as high.
    ALTER TABLE t4 DELETE WHERE c0 = 1 SETTINGS mutations_sync = 2;

    DETACH TABLE t4 SYNC;
"
t4_error=$(${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null --query="ATTACH TABLE t4 AS REPLICATED" 2>&1)
echo "$t4_error" | grep -c 'Cannot ATTACH AS REPLICATED'
echo "$t4_error" | grep -c 'all_1_2_1'
# Nothing was modified: the table still attaches as a MergeTree, and the part loader resolves the
# same intersection correctly while the transaction metadata is still there.
${CLICKHOUSE_CLIENT} -n -q "
    ATTACH TABLE t4;

    SELECT count() FROM t4;
    SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't4';

    DROP TABLE t4;
"

# test5: a rolled-back insert that no active part covers. Same root cause, observable as wrong results:
# stripping its transaction metadata used to promote it to a committed part and resurrect its rows.
# The same cleanup pins as t4: the rolled-back all_2_2_0 has to still be on disk at ATTACH time.
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE t5 (c0 Int) ENGINE = MergeTree() PRIMARY KEY tuple()
    SETTINGS old_parts_lifetime = 10000, max_bytes_to_merge_at_max_space_in_pool = 0,
             remove_rolled_back_parts_immediately = 0,
             merge_tree_clear_old_parts_interval_seconds = 100000,
             cleanup_delay_period = 100000, max_cleanup_delay_period = 100000;
    INSERT INTO TABLE t5 (c0) VALUES (1);

    BEGIN TRANSACTION;
    INSERT INTO TABLE t5 (c0) VALUES (2);
    ROLLBACK;

    DETACH TABLE t5 SYNC;
"
t5_error=$(${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null --query="ATTACH TABLE t5 AS REPLICATED" 2>&1)
echo "$t5_error" | grep -c 'Cannot ATTACH AS REPLICATED'
echo "$t5_error" | grep -c 'all_2_2_0'
${CLICKHOUSE_CLIENT} -n -q "
    ATTACH TABLE t5;

    SELECT count() FROM t5;

    DROP TABLE t5;
"

# test6: a committed transactional DROP PARTITION records the removal on the dropped partition's part
# and creates no replacement part, so no active part covers it. That record is the only thing keeping
# the part out of the table's data: removing it makes the part look committed again and brings the
# dropped rows back. Same cleanup pins as t4 and t5, so the part is still on disk at ATTACH time.
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE t6 (p Int, c0 Int) ENGINE = MergeTree() PARTITION BY p ORDER BY c0
    SETTINGS old_parts_lifetime = 10000, max_bytes_to_merge_at_max_space_in_pool = 0,
             remove_rolled_back_parts_immediately = 0,
             merge_tree_clear_old_parts_interval_seconds = 100000,
             cleanup_delay_period = 100000, max_cleanup_delay_period = 100000;
    INSERT INTO TABLE t6 (p, c0) VALUES (1, 10);
    INSERT INTO TABLE t6 (p, c0) VALUES (2, 20);

    BEGIN TRANSACTION;
    ALTER TABLE t6 DROP PARTITION 2;
    COMMIT;

    DETACH TABLE t6 SYNC;
"
t6_error=$(${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null --query="ATTACH TABLE t6 AS REPLICATED" 2>&1)
echo "$t6_error" | grep -c 'Cannot ATTACH AS REPLICATED'
echo "$t6_error" | grep -c '2_2_2_0'
${CLICKHOUSE_CLIENT} -n -q "
    ATTACH TABLE t6;

    SELECT count() FROM t6;

    DROP TABLE t6;
"
