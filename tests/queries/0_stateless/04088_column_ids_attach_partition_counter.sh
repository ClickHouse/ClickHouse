#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-parallel-replicas
# Scenario: ATTACH PARTITION FROM between two column-IDs tables must check that
# the source's `next_column_id` counter is not ahead of the destination's.
# Otherwise source parts may carry orphan column files at IDs the destination
# would later hand out via ADD COLUMN, resulting in stale-orphan reads.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1"

$CLIENT --query "DROP TABLE IF EXISTS t_attach_src SYNC"
$CLIENT --query "DROP TABLE IF EXISTS t_attach_dst SYNC"

$CLIENT --query "
CREATE TABLE t_attach_src (a UInt32, b String, c Float64)
ENGINE = MergeTree PARTITION BY tuple() ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
"
# Push src counter ahead by adding then dropping a temporary column.
$CLIENT --query "ALTER TABLE t_attach_src ADD COLUMN d UInt32 DEFAULT 0"
echo "INSERT INTO t_attach_src (a, b, c, d) VALUES (1, 'x', 1.5, 42)" | $CLIENT
$CLIENT --query "ALTER TABLE t_attach_src DROP COLUMN d"

$CLIENT --query "
CREATE TABLE t_attach_dst (a UInt32, b String, c Float64)
ENGINE = MergeTree PARTITION BY tuple() ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
"
echo "INSERT INTO t_attach_dst VALUES (10, 'y', 99)" | $CLIENT
# At this point both tables have logical_to_id = {a:a,b:b,c:c}, but src's
# counter is past the orphan d's ID while dst's counter is still at 1.

echo "== ATTACH PARTITION with counter mismatch =="
$CLIENT --query "
ALTER TABLE t_attach_dst ATTACH PARTITION tuple() FROM t_attach_src
" 2>&1 | grep -qE "column-ID counter \([0-9]+\) is ahead" && echo "throws_on_counter_mismatch" || echo "missing_guard"

# Destination still readable with its own mapping.
$CLIENT --query "SELECT a, b, c FROM t_attach_dst ORDER BY a"

$CLIENT --query "DROP TABLE t_attach_src SYNC"
$CLIENT --query "DROP TABLE t_attach_dst SYNC"
