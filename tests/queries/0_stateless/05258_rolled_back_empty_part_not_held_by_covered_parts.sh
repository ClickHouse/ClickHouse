#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-shared-merge-tree
# no-parallel: the fail point is global, and a `TRUNCATE` of another test could hit it.
# no-fasttest: tests that arm a fail point must not run in the fast test, where they would run alone.
# no-shared-merge-tree: the fail point is in `StorageMergeTree`.

# The empty parts written by a `TRUNCATE` that failed to commit must be removed from disk right away. Nothing on disk
# marks such a part as rolled back, so if it stayed, a restart would load it as a covering part and resurrect the
# failed `TRUNCATE`. The background cleanup cannot remove it promptly either, because an empty part waits for the
# outdated parts inside its range, and a merge could meanwhile write a part intersecting it, so the table then failed
# to load with `Part ... intersects previous part ...`. `SYSTEM STOP CLEANUP` checks that the removal does not depend
# on the background cleanup.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (n Int64) ENGINE = MergeTree ORDER BY n SETTINGS old_parts_lifetime = 3600;
    SYSTEM STOP CLEANUP t;
    SYSTEM STOP MERGES t;
    INSERT INTO t VALUES (1);
    INSERT INTO t VALUES (2);
"

# The fail point is `ONCE`, but disarm it on every exit path in case `TRUNCATE` did not reach it.
trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_throw_after_renaming_empty_parts" ||:' EXIT

# Writes the empty parts `all_1_1_1` and `all_2_2_1`, then fails and rolls them back.
$CLICKHOUSE_CLIENT -q "
    SYSTEM ENABLE FAILPOINT mt_throw_after_renaming_empty_parts;
    TRUNCATE TABLE t; -- { serverError FAULT_INJECTED }
"

$CLICKHOUSE_CLIENT -q "
    SELECT name, active, rows FROM system.parts WHERE database = currentDatabase() AND table = 't' ORDER BY name;
    DETACH TABLE t;
    ATTACH TABLE t;
    SELECT n FROM t ORDER BY n;
"

# Writes `all_1_2_1`, which would intersect the rolled back parts if they were still on disk.
$CLICKHOUSE_CLIENT -q "
    SYSTEM STOP CLEANUP t;
    OPTIMIZE TABLE t FINAL;
    SELECT name, active, rows FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active ORDER BY name;
    DETACH TABLE t;
    ATTACH TABLE t;
    SELECT n FROM t ORDER BY n;
    DROP TABLE t;
"
