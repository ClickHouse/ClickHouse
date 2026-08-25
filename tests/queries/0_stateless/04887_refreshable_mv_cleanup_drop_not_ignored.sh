#!/usr/bin/env bash
# Tags: zookeeper, no-ordinary-database, no-replicated-database
# no-ordinary-database: a refreshable materialized view rotates its target with CREATE OR REPLACE, which needs Atomic.
# no-replicated-database: the last part of the test creates a Replicated database itself.

# The DROP that a refreshable materialized view issues to clean up its rotated-out target
# (StorageMaterializedView::dropTempTable) is a step of the refresh, not a user DROP, so
# `ignore_drop_queries_probability` must not skip it. When it does, the old target survives
# under its internal `.tmp.inner_id.<uuid>` name, still holding a full copy of the view's data,
# outside the view's metadata and past a restart, and one leaks per refresh.
#
# The setting is pinned in a SETTINGS clause on the view's SELECT: that is what reaches the
# background refresh context, whereas a session-level SET does not.
#
# Objects live in databases the test creates, torn down with DROP DATABASE, which is not
# injected -- with the setting pinned, a plain DROP TABLE teardown would itself be skipped.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo '-- success path: the rotated-out target must be dropped, not leaked'

db="${CLICKHOUSE_DATABASE}_ok"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${db}"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${db}.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${db}.src SELECT number FROM numbers(1000);

    CREATE MATERIALIZED VIEW ${db}.rmv
        REFRESH EVERY 1 YEAR
        (x UInt64) ENGINE = MergeTree ORDER BY x EMPTY
        AS SELECT * FROM ${db}.src SETTINGS ignore_drop_queries_probability = 1;
"

# Refresh #1 rotates out the initial empty target; refresh #2 rotates out a populated one.
${CLICKHOUSE_CLIENT} -q "
    SYSTEM REFRESH VIEW ${db}.rmv;
    SYSTEM WAIT VIEW ${db}.rmv;
    SYSTEM REFRESH VIEW ${db}.rmv;
    SYSTEM WAIT VIEW ${db}.rmv;
"

${CLICKHOUSE_CLIENT} -q "SELECT countIf(name LIKE '.tmp.%') FROM system.tables WHERE database = '${db}'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${db}.rmv"

echo '-- failure path: the unpublished temporary target must be dropped too'

db_fail="${CLICKHOUSE_DATABASE}_fail"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${db_fail}"
# One row per block, so blocks land as parts in the temporary target before `throwIf` fires:
# the refresh then fails with the target already created and populated, which is the cleanup
# path taken from the catch branch rather than after a successful swap.
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${db_fail}.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${db_fail}.src SELECT number FROM numbers(100);

    CREATE MATERIALIZED VIEW ${db_fail}.rmv
        REFRESH EVERY 1 YEAR
        (x UInt64) ENGINE = MergeTree ORDER BY x EMPTY
        AS SELECT throwIf(x = 50, 'stop') AS x FROM ${db_fail}.src
        SETTINGS ignore_drop_queries_probability = 1, max_insert_block_size = 1,
                 min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1, max_block_size = 1;
"
${CLICKHOUSE_CLIENT} -q "SYSTEM REFRESH VIEW ${db_fail}.rmv"
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT VIEW ${db_fail}.rmv" 2>&1 | grep -q 'REFRESH_FAILED' && echo 'refresh_failed'
${CLICKHOUSE_CLIENT} -q "SELECT countIf(name LIKE '.tmp.%') FROM system.tables WHERE database = '${db_fail}'"

echo '-- Replicated database: the cleanup DROP is enqueued, not injected'

# The injection is checked before the query is enqueued for replication, so the leak is
# reachable here too, and the fix must keep the DROP working through the enqueue path.
db_repl="${CLICKHOUSE_DATABASE}_repl"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    CREATE DATABASE ${db_repl}
        ENGINE = Replicated('/test/{database}/refreshable_mv_cleanup_drop', 'shard1', 'replica1');
"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    CREATE TABLE ${db_repl}.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${db_repl}.src SELECT number FROM numbers(1000);

    CREATE MATERIALIZED VIEW ${db_repl}.rmv
        REFRESH EVERY 1 YEAR
        (x UInt64) ENGINE = ReplicatedMergeTree ORDER BY x EMPTY
        AS SELECT * FROM ${db_repl}.src SETTINGS ignore_drop_queries_probability = 1;
"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    SYSTEM REFRESH VIEW ${db_repl}.rmv;
    SYSTEM WAIT VIEW ${db_repl}.rmv;
    SYSTEM REFRESH VIEW ${db_repl}.rmv;
    SYSTEM WAIT VIEW ${db_repl}.rmv;
"
${CLICKHOUSE_CLIENT} -q "SELECT countIf(name LIKE '.tmp.%') FROM system.tables WHERE database = '${db_repl}'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${db_repl}.rmv"

echo '-- a user DROP is still skipped: the setting keeps doing what it is for'

${CLICKHOUSE_CLIENT} -q "
    SET ignore_drop_queries_probability = 1;
    CREATE TABLE ${db}.user_drop (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO ${db}.user_drop SELECT number FROM numbers(10);
    DROP TABLE ${db}.user_drop;
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${db}' AND name = 'user_drop'"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${db}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${db_fail}"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "DROP DATABASE ${db_repl}"
