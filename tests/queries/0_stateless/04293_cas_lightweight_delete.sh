#!/usr/bin/env bash
# Tags: no-fasttest
# ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

# Correctness oracle for lightweight DELETE on content-addressed disks (CAS M7).
# After the supportsHardLinks() gate was lifted, lightweight DELETE is enabled on
# content-addressed disks.  Unlike heavy mutations, lightweight DELETE uses row-
# existence bitmaps stored alongside each part and is applied physically during
# the next OPTIMIZE/merge.
#
# Strategy: both tables receive identical data and identical lightweight DELETEs;
# after each DELETE (and after OPTIMIZE FINAL) we assert the full ordered contents
# are equal (CA vs plain MergeTree).  Every assertion is a self-checking CA-vs-plain
# equality so the reference file is trivially correct (no hand-computed arithmetic
# needed).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DISK_CA="disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '04293',
    name = '04293_cas_lwd',
    path = '04293_cas_lwd_pool/')"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_ca    SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_plain SYNC"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_ca (id UInt64, v UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS disk = ${DISK_CA}"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_plain (id UInt64, v UInt64, s String)
ENGINE = MergeTree ORDER BY id"

# Seed both tables with identical deterministic data spread across two parts
# (lightweight DELETEs across multiple parts are more meaningful than single-part).
$CLICKHOUSE_CLIENT --query "
INSERT INTO t_ca    SELECT number, number * 10, toString(number) FROM numbers(100)"
$CLICKHOUSE_CLIENT --query "
INSERT INTO t_ca    SELECT number, number * 10, toString(number) FROM numbers(100, 100)"
$CLICKHOUSE_CLIENT --query "
INSERT INTO t_plain SELECT number, number * 10, toString(number) FROM numbers(100)"
$CLICKHOUSE_CLIENT --query "
INSERT INTO t_plain SELECT number, number * 10, toString(number) FROM numbers(100, 100)"

# Helper: compare full ordered contents of both tables.
CMP_QUERY="SELECT if(
  (SELECT groupArray((id, v, s)) FROM (SELECT id, v, s FROM t_ca    ORDER BY id)) =
  (SELECT groupArray((id, v, s)) FROM (SELECT id, v, s FROM t_plain ORDER BY id)),
  'match', 'DIFF')"

# --- Lightweight DELETE 1: every 5th row ---
$CLICKHOUSE_CLIENT --query "
DELETE FROM t_ca    WHERE id % 5 = 0 SETTINGS lightweight_deletes_sync = 2"
$CLICKHOUSE_CLIENT --query "
DELETE FROM t_plain WHERE id % 5 = 0 SETTINGS lightweight_deletes_sync = 2"

echo -n 'after_delete_mod5: '
$CLICKHOUSE_CLIENT --query "$CMP_QUERY"

# --- Lightweight DELETE 2: rows whose string starts with '1' (overlaps first delete) ---
$CLICKHOUSE_CLIENT --query "
DELETE FROM t_ca    WHERE s LIKE '1%' SETTINGS lightweight_deletes_sync = 2"
$CLICKHOUSE_CLIENT --query "
DELETE FROM t_plain WHERE s LIKE '1%' SETTINGS lightweight_deletes_sync = 2"

echo -n 'after_delete_like: '
$CLICKHOUSE_CLIENT --query "$CMP_QUERY"

# --- Lightweight DELETE 3: rows with v > 1500 ---
$CLICKHOUSE_CLIENT --query "
DELETE FROM t_ca    WHERE v > 1500 SETTINGS lightweight_deletes_sync = 2"
$CLICKHOUSE_CLIENT --query "
DELETE FROM t_plain WHERE v > 1500 SETTINGS lightweight_deletes_sync = 2"

echo -n 'after_delete_range: '
$CLICKHOUSE_CLIENT --query "$CMP_QUERY"

# --- OPTIMIZE FINAL: force merge so lightweight deletes are physically applied ---
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_ca    FINAL"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_plain FINAL"

echo -n 'after_optimize: '
$CLICKHOUSE_CLIENT --query "$CMP_QUERY"

# --- Final sanity: row count and data equality ---
$CLICKHOUSE_CLIENT --query "
SELECT 'final_rows_match', count() = (SELECT count() FROM t_plain) FROM t_ca"
$CLICKHOUSE_CLIENT --query "
SELECT 'final_data_match',
  (SELECT groupArray((id, v, s)) FROM (SELECT id, v, s FROM t_ca    ORDER BY id)) =
  (SELECT groupArray((id, v, s)) FROM (SELECT id, v, s FROM t_plain ORDER BY id))"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_ca    SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_plain SYNC"
