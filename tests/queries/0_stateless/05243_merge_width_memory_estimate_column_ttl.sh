#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A merge that clears expired columns is chosen by `TTLColumnDeleteMergeSelector`, and it holds one block
# from every source part at once like any other merge. `merge_memory_estimate_per_source_part_column` has
# to narrow it too: otherwise a wide table on a small server keeps selecting the same too-wide column TTL
# merge, which fails with `MEMORY_LIMIT_EXCEEDED` every time. An absurdly large estimate makes even three
# columns exceed the budget, so the merge width falls to its floor of two parts.
#
# The table has column TTLs and no rows TTL, so the column TTL selector is the only one with work to do:
# the TTL of `v` is already due, while the TTL of `w` is far in the future and keeps the parts from
# counting as fully expired, which would hand them to the part drop selector instead.
# `merge_with_ttl_timeout = 0` lets TTL merges run back to back, so background merges may take some of
# the parts before `OPTIMIZE` does; every TTL merge, whoever runs it, has to be narrowed. When the TTL merge
# slots of the server are all busy with other tables, ordinary merges do the work instead, and they are
# narrowed as well - so the test checks the width of every merge, whatever its reason.

# A background merge may still be running when `OPTIMIZE` finds nothing left to take, and its `part_log`
# entry appears only when it finishes.
function wait_for_merges()
{
    local table=$1
    for _ in {1..600}
    do
        [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = '$table'")" = 0 ] && return
        sleep 0.1
    done
    echo "Merges of $table did not finish"
}

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_merge_width_column_ttl;

CREATE TABLE t_merge_width_column_ttl (k UInt64, d DateTime, v UInt64 TTL d + INTERVAL 1 SECOND, w UInt64 TTL d + INTERVAL 1000 YEAR)
ENGINE = MergeTree ORDER BY k
SETTINGS merge_memory_estimate_per_source_part_column = 1000000000000,
    min_parts_to_merge_at_once = 2,
    merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0,
    merge_with_ttl_timeout = 0;

SYSTEM STOP MERGES t_merge_width_column_ttl;
INSERT INTO t_merge_width_column_ttl VALUES (1, '2000-01-01 00:00:00', 1, 1);
INSERT INTO t_merge_width_column_ttl VALUES (2, '2000-01-01 00:00:00', 2, 2);
INSERT INTO t_merge_width_column_ttl VALUES (3, '2000-01-01 00:00:00', 3, 3);
INSERT INTO t_merge_width_column_ttl VALUES (4, '2000-01-01 00:00:00', 4, 4);
INSERT INTO t_merge_width_column_ttl VALUES (5, '2000-01-01 00:00:00', 5, 5);
INSERT INTO t_merge_width_column_ttl VALUES (6, '2000-01-01 00:00:00', 6, 6);
SELECT 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_column_ttl' AND active;
SYSTEM START MERGES t_merge_width_column_ttl;

OPTIMIZE TABLE t_merge_width_column_ttl;
"

wait_for_merges t_merge_width_column_ttl

$CLICKHOUSE_CLIENT --query "
SELECT sum(k), sum(w), count() FROM t_merge_width_column_ttl;

SYSTEM FLUSH LOGS part_log;

SELECT DISTINCT length(merged_from) FROM system.part_log
WHERE database = currentDatabase() AND table = 't_merge_width_column_ttl' AND event_type = 'MergeParts';

DROP TABLE t_merge_width_column_ttl;
"
