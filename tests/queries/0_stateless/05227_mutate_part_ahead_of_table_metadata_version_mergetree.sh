#!/usr/bin/env bash
# Tags: no-shared-merge-tree
# Tag no-shared-merge-tree: the engine below is substituted for one that supports replication, for
#                           which a part ahead of the table metadata version is a logical error

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# A `MergeTree` table has no metadata version to reason with, so a part whose own metadata version is
# ahead of the table's - what a part carried over from a `ReplicatedMergeTree` table looks like - can
# still carry a column the table does not have. Mutating that part has to drop the column instead of
# reading it, or the mutation fails and every later mutation of the table fails with it.

WORKING_DIR=$(mktemp -d -p "${CLICKHOUSE_TMP}" clickhouse.05227.XXXXXX)
trap 'rm -rf "${WORKING_DIR}"' EXIT

local_query() { ${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --query "$1"; }

local_query "
    CREATE TABLE t_part_ahead (id UInt64, val UInt64, p UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, min_bytes_for_full_part_storage = 0;
    ALTER TABLE t_part_ahead ADD COLUMN c UInt32;
    INSERT INTO t_part_ahead SELECT number, number, 1, 42 FROM numbers(100);
    ALTER TABLE t_part_ahead DETACH PARTITION 1;
    ALTER TABLE t_part_ahead DROP COLUMN c;
"

DETACHED_DIR=$(local_query "SELECT path FROM system.detached_parts WHERE table = 't_part_ahead'")
chmod u+w "${DETACHED_DIR%/}/metadata_version.txt"
printf '1' > "${DETACHED_DIR%/}/metadata_version.txt"

local_query "ALTER TABLE t_part_ahead ATTACH PARTITION 1"

PART_DIR=$(local_query "SELECT path FROM system.parts WHERE table = 't_part_ahead' AND active")

echo -n 'the part metadata version: '
cat "${PART_DIR%/}/metadata_version.txt"
echo
echo -n 'the table metadata version: '
local_query "SELECT metadata_version FROM system.tables WHERE name = 't_part_ahead'"
echo -n 'the part type: '
local_query "SELECT any(part_type) FROM system.parts WHERE table = 't_part_ahead' AND active"
echo -n 'the part still has the dropped column: '
local_query "SELECT countIf(column = 'c') FROM system.parts_columns WHERE table = 't_part_ahead' AND active"

local_query "ALTER TABLE t_part_ahead DELETE WHERE id = 5 SETTINGS mutations_sync = 2"

echo -n 'rows after the delete: '
local_query "SELECT count() FROM t_part_ahead"
echo -n 'the rewrite dropped the column: '
local_query "SELECT countIf(column = 'c') FROM system.parts_columns WHERE table = 't_part_ahead' AND active"
echo -n 'unfinished mutations: '
local_query "SELECT count() FROM system.mutations WHERE table = 't_part_ahead' AND NOT is_done"
echo -n 'the rewritten part is consistent: '
local_query "CHECK TABLE t_part_ahead SETTINGS check_query_single_value_result = 1"

local_query "DROP TABLE t_part_ahead"
