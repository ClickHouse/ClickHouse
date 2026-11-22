#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-shared-merge-tree
# no-parallel: the fail point is global, and a `TRUNCATE` or `MOVE PARTITION` of another test could hit it.
# no-fasttest: tests that arm a fail point must not run in the fast test, where they would run alone.
# no-shared-merge-tree: the fail point is in `StorageMergeTree`.

# The empty parts written to cover the source parts of a `MOVE PARTITION TO TABLE` that failed to commit them must be
# removed from disk right away. Nothing on disk marks such a part as rolled back, so if it stayed, a restart would load
# it as a covering part and hide the source partition, and a merge could write a part intersecting it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS src;
    DROP TABLE IF EXISTS dst;
    CREATE TABLE src (n Int64) ENGINE = MergeTree ORDER BY n SETTINGS old_parts_lifetime = 3600;
    CREATE TABLE dst (n Int64) ENGINE = MergeTree ORDER BY n;
    SYSTEM STOP CLEANUP src;
    SYSTEM STOP MERGES src;
    INSERT INTO src VALUES (1);
    INSERT INTO src VALUES (2);
"

# The fail point is `ONCE`, but disarm it on every exit path in case `MOVE PARTITION` did not reach it.
trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_throw_after_renaming_empty_parts" ||:' EXIT

# Commits the parts in `dst`, writes the empty parts `all_1_1_1` and `all_2_2_1` in `src`, then fails and rolls them back.
$CLICKHOUSE_CLIENT -q "
    SYSTEM ENABLE FAILPOINT mt_throw_after_renaming_empty_parts;
    ALTER TABLE src MOVE PARTITION tuple() TO TABLE dst; -- { serverError FAULT_INJECTED }
"

$CLICKHOUSE_CLIENT -q "
    SELECT name, active, rows FROM system.parts WHERE database = currentDatabase() AND table = 'src' ORDER BY name;
    DETACH TABLE src;
    ATTACH TABLE src;
    SELECT n FROM src ORDER BY n;
"

# Writes `all_1_2_1`, which would intersect the rolled back parts if they were still on disk.
$CLICKHOUSE_CLIENT -q "
    SYSTEM STOP CLEANUP src;
    OPTIMIZE TABLE src FINAL;
    SELECT name, active, rows FROM system.parts WHERE database = currentDatabase() AND table = 'src' AND active ORDER BY name;
    DETACH TABLE src;
    ATTACH TABLE src;
    SELECT n FROM src ORDER BY n;
    DROP TABLE src;
    DROP TABLE dst;
"
