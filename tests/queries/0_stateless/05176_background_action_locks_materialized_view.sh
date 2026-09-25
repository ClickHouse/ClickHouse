#!/usr/bin/env bash
# Tags: atomic-database, memory-engine

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An ordinary `MaterializedView` forwards `STOP MERGES` to its implicit `MergeTree` table.
inner=$($CLICKHOUSE_CLIENT --multiquery -q "
    CREATE TABLE src (value UInt64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY value AS SELECT value FROM src;
    SYSTEM STOP MERGES mv;
    INSERT INTO src VALUES (1);
    INSERT INTO src VALUES (2);
    SELECT target_table FROM system.tables WHERE database = currentDatabase() AND name = 'mv';")

# `DETACH ... SYNC` destroys the view, but its inner table and forwarded blocker survive.
# The next control invokes `ActionLocksManager::cleanExpired`, releasing the expired
# view owner's lock. This does not require automatic cleanup at destruction or address reuse.
# `START` on the inner table alone cannot remove a lock registered under the view's identity.
$CLICKHOUSE_CLIENT --param_inner="$inner" --multiquery -q "
    SELECT 'stopped parts', count() = 2 AND sum(rows) = 2
        FROM system.parts WHERE database = currentDatabase() AND table = {inner:String} AND active;
    DETACH TABLE mv SYNC;
    SYSTEM START MERGES {inner:Identifier};
    OPTIMIZE TABLE {inner:Identifier} FINAL SETTINGS optimize_skip_merged_partitions = 0;
    SELECT 'resumed parts', count() = 1 AND sum(rows) = 2
        FROM system.parts WHERE database = currentDatabase() AND table = {inner:String} AND active;
    SELECT 'preserved rows', groupArray(value) = [1, 2]
        FROM (SELECT value FROM {inner:Identifier} ORDER BY value);
    ATTACH TABLE mv;
    DROP TABLE mv SYNC;
    DROP TABLE src SYNC;"
