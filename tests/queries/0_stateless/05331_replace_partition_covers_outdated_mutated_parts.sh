#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree
# no-replicated-database: the test relies on the block numbers and the mutation version of a single, freshly created table.
# no-shared-merge-tree: the test is about the covering part that plain `MergeTree` writes on `REPLACE PARTITION`.

# `REPLACE PARTITION` into a plain `MergeTree` table during an unfinished mutation: an outdated part left by the
# mutation has a higher mutation version than the remaining active part, and the table must still attach.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_replace_cover_mut_dst;
    DROP TABLE IF EXISTS t_replace_cover_mut_src;

    CREATE TABLE t_replace_cover_mut_dst (p UInt64, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k
    SETTINGS old_parts_lifetime = 100000, merge_tree_clear_old_parts_interval_seconds = 100000, remove_empty_parts = 1;

    CREATE TABLE t_replace_cover_mut_src (p UInt64, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;

    INSERT INTO t_replace_cover_mut_dst SETTINGS async_insert = 0 VALUES (1, 1);
    INSERT INTO t_replace_cover_mut_dst SETTINGS async_insert = 0 VALUES (1, 2);

    -- The mutation empties \`1_2_2_0\` into \`1_2_2_0_3\` and keeps failing on \`1_1_1_0\`.
    ALTER TABLE t_replace_cover_mut_dst DELETE WHERE k = 2 OR throwIf(k = 1) SETTINGS mutations_sync = 0;
"

for _ in {1..600}
do
    [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_replace_cover_mut_dst' AND name = '1_2_2_0_3' AND active") == 1 ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "
    -- Dropping a missing partition clears empty parts, so \`1_2_2_0_3\` stays on disk as an outdated part nothing active covers.
    ALTER TABLE t_replace_cover_mut_dst DROP PARTITION 2;

    INSERT INTO t_replace_cover_mut_src SETTINGS async_insert = 0 VALUES (1, 3);

    ALTER TABLE t_replace_cover_mut_dst REPLACE PARTITION 1 FROM t_replace_cover_mut_src;

    SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_replace_cover_mut_dst' AND name = '1_2_2_0_3' AND NOT active;

    DETACH TABLE t_replace_cover_mut_dst;
    ATTACH TABLE t_replace_cover_mut_dst;

    SELECT * FROM t_replace_cover_mut_dst ORDER BY k;

    DROP TABLE t_replace_cover_mut_dst;
    DROP TABLE t_replace_cover_mut_src;
"
