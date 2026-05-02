#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree
# Test: exercises the FALLBACK path in checkDataPart and MergeTreeDataPartWide::doCheckConsistency
# when a wide part with a Dynamic column does NOT have columns_substreams.txt
# (simulates an "old part" written by a ClickHouse version before columns_substreams.txt was introduced).
# Covers:
#   src/Storages/MergeTree/checkDataPart.cpp:286-308 — fallback enumerateStreams(enumerate_dynamic_streams=false)
#   src/Storages/MergeTree/MergeTreeDataPartWide.cpp:384-417 — same fallback in doCheckConsistency
# Without enumerate_dynamic_streams=false, enumerateStreams on a Dynamic column without
# deserialization state yields wrong stream names → "No file for column" false positives.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_check_dynamic_old;"

$CLICKHOUSE_CLIENT -q "
SET allow_experimental_dynamic_type = 1;
CREATE TABLE test_check_dynamic_old (id UInt64, data Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part=1, min_bytes_for_wide_part=1;
"

$CLICKHOUSE_CLIENT -q "INSERT INTO test_check_dynamic_old VALUES (1, 1::Int64);"
$CLICKHOUSE_CLIENT -q "INSERT INTO test_check_dynamic_old VALUES (2, 'hello');"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE test_check_dynamic_old FINAL;"

# Locate the active part directory.
path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.parts WHERE database='${CLICKHOUSE_DATABASE}' AND table='test_check_dynamic_old' AND active=1 LIMIT 1")
$CLICKHOUSE_CLIENT -q "SELECT throwIf(substring('$path', 1, 1) != '/', 'Path is relative: $path')" >/dev/null || exit 1

# Detach so the part is unloaded from memory; then physically remove columns_substreams.txt
# (the file is not listed in checksums.txt — it's a metadata-only file).
$CLICKHOUSE_CLIENT -q "DETACH TABLE test_check_dynamic_old;"
rm -f "$path/columns_substreams.txt"

# Attach: triggers checkConsistency → MergeTreeDataPartWide::doCheckConsistency,
# which now enters the FALLBACK branch because cols_substreams is empty.
$CLICKHOUSE_CLIENT -q "ATTACH TABLE test_check_dynamic_old;"

# CHECK TABLE: triggers checkDataPart, which also enters the FALLBACK branch.
$CLICKHOUSE_CLIENT -q "CHECK TABLE test_check_dynamic_old SETTINGS check_query_single_value_result = 1;"

# Verify data is still readable.
$CLICKHOUSE_CLIENT -q "SELECT id, toString(data) FROM test_check_dynamic_old ORDER BY id;"

$CLICKHOUSE_CLIENT -q "DROP TABLE test_check_dynamic_old;"
