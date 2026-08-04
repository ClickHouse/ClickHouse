#!/usr/bin/env bash
# Tags: no-fasttest
# ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

# Correctness oracle for PATCH PARTS (the native lightweight-update model, B5) on content-addressed
# disks (CAS M7). The default lightweight DELETE mode is `alter_update` (a heavy mutation); this test
# forces the lightweight-update path with `lightweight_delete_mode = 'lightweight_update_force'`, which
# produces a PATCH PART (an `UPDATE _row_exists = 0`). `_force` THROWS if the table cannot do a
# lightweight update, so a successful run is itself proof the patch-part path was exercised — on a
# content-addressed disk the patch part is written through the same whole-part transaction as any part.
#
# Lightweight updates require materialized `_block_number` / `_block_offset` columns
# (enable_block_number_column / enable_block_offset_column) and a non-UNIQUE-KEY custom-partitioned
# table. Both tables get identical settings, data, and operations; every assertion is a self-checking
# CA-vs-plain equality so the reference file is trivially correct.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DISK_CA="disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '04294',
    name = '04294_cas_patch',
    path = '04294_cas_patch_pool/')"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_ca    SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_plain SYNC"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_ca (id UInt64, v UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS disk = ${DISK_CA}, enable_block_number_column = 1, enable_block_offset_column = 1"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_plain (id UInt64, v UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1"

# Seed both identically across two parts.
$CLICKHOUSE_CLIENT --query "INSERT INTO t_ca    SELECT number, number * 10, toString(number) FROM numbers(100)"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_ca    SELECT number, number * 10, toString(number) FROM numbers(100, 100)"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_plain SELECT number, number * 10, toString(number) FROM numbers(100)"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_plain SELECT number, number * 10, toString(number) FROM numbers(100, 100)"

CMP_QUERY="SELECT if(
  (SELECT groupArray((id, v, s)) FROM (SELECT id, v, s FROM t_ca    ORDER BY id)) =
  (SELECT groupArray((id, v, s)) FROM (SELECT id, v, s FROM t_plain ORDER BY id)),
  'match', 'DIFF')"

# Force the patch-part (lightweight-update) path. `_force` throws if unsupported, so success == patch path.
LWU_SETTINGS="SETTINGS enable_lightweight_update = 1, lightweight_delete_mode = 'lightweight_update_force', lightweight_deletes_sync = 2"

# --- Patch-part DELETE 1 ---
$CLICKHOUSE_CLIENT --query "DELETE FROM t_ca    WHERE id % 5 = 0 ${LWU_SETTINGS}"
$CLICKHOUSE_CLIENT --query "DELETE FROM t_plain WHERE id % 5 = 0 ${LWU_SETTINGS}"
echo -n 'after_patch_delete_1: '
$CLICKHOUSE_CLIENT --query "$CMP_QUERY"

# --- Patch-part DELETE 2 (overlaps the first) ---
$CLICKHOUSE_CLIENT --query "DELETE FROM t_ca    WHERE v > 1500 ${LWU_SETTINGS}"
$CLICKHOUSE_CLIENT --query "DELETE FROM t_plain WHERE v > 1500 ${LWU_SETTINGS}"
echo -n 'after_patch_delete_2: '
$CLICKHOUSE_CLIENT --query "$CMP_QUERY"

# Prove a patch part really exists on the content-addressed table before it is merged away.
echo -n 'ca_has_patch_part: '
$CLICKHOUSE_CLIENT --query "
SELECT count() > 0 FROM system.parts
WHERE database = currentDatabase() AND table = 't_ca' AND active AND startsWith(name, 'patch')"

# --- OPTIMIZE FINAL applies the patch parts during merge ---
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_ca    FINAL"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_plain FINAL"
echo -n 'after_optimize: '
$CLICKHOUSE_CLIENT --query "$CMP_QUERY"

# --- Final equality ---
$CLICKHOUSE_CLIENT --query "SELECT 'final_rows_match', count() = (SELECT count() FROM t_plain) FROM t_ca"
$CLICKHOUSE_CLIENT --query "
SELECT 'final_data_match',
  (SELECT groupArray((id, v, s)) FROM (SELECT id, v, s FROM t_ca    ORDER BY id)) =
  (SELECT groupArray((id, v, s)) FROM (SELECT id, v, s FROM t_plain ORDER BY id))"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_ca    SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_plain SYNC"
