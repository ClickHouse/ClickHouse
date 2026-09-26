#!/usr/bin/env bash
# Tags: no-parallel, no-shared-merge-tree
# no-parallel: the fail point is global, and a `TRUNCATE` of another test could hit it.
# no-shared-merge-tree: the fail point is in `StorageMergeTree`, and `SYSTEM STOP CLEANUP` is needed to keep the
# rolled back parts until the parts inside their ranges become outdated.

# The empty parts written by a `TRUNCATE` that failed to commit must be removed promptly, even when outdated parts
# inside their ranges are still on disk: they never covered anything. They used to wait for `old_parts_lifetime`,
# and a merge could meanwhile write a part intersecting them. Nothing on disk marks such a part as rolled back,
# so the table then failed to load with `Part ... intersects previous part ...`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (n Int64) ENGINE = MergeTree ORDER BY n
        SETTINGS old_parts_lifetime = 3600,
            cleanup_delay_period = 1, cleanup_delay_period_random_add = 0, max_cleanup_delay_period = 1;
    SYSTEM STOP CLEANUP t;
    SYSTEM STOP MERGES t;
    INSERT INTO t VALUES (1);
    INSERT INTO t VALUES (2);
"

# Writes the empty parts `all_1_1_1` and `all_2_2_1`, then fails and rolls them back.
$CLICKHOUSE_CLIENT -q "
    SYSTEM ENABLE FAILPOINT mt_throw_after_renaming_empty_parts;
    TRUNCATE TABLE t; -- { serverError FAULT_INJECTED }
"

# Outdates `all_1_1_0` and `all_2_2_0`, which are inside the ranges of the rolled back parts, and writes
# `all_1_2_1`, which intersects them.
$CLICKHOUSE_CLIENT -q "
    SYSTEM START MERGES t;
    OPTIMIZE TABLE t FINAL;
    SELECT name, active, rows FROM system.parts WHERE database = currentDatabase() AND table = 't' ORDER BY name;
    SYSTEM START CLEANUP t;
"

for _ in {1..600}
do
    remaining=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.parts
        WHERE database = currentDatabase() AND table = 't' AND name IN ('all_1_1_1', 'all_2_2_1')")
    [ "$remaining" = "0" ] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "
    SELECT name, removal_state FROM system.parts
    WHERE database = currentDatabase() AND table = 't' AND name IN ('all_1_1_1', 'all_2_2_1')
    ORDER BY name;
    DETACH TABLE t;
    ATTACH TABLE t;
    SELECT n FROM t ORDER BY n;
    DROP TABLE t;
"
