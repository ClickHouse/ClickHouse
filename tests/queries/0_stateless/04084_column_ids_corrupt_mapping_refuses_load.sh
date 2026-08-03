#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-parallel-replicas, no-object-storage, no-replicated-database, no-shared-merge-tree, no-async-insert
# why: a with_column_ids table must refuse to load when column_ids.json is
# incomplete or absent -- auto-rebuild is unsafe because DROP + re-ADD makes
# on-disk files indistinguishable from their column ID alone.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1"

# Scenario 1: the mapping is present but misses an entry for a column that still
# exists in metadata; ATTACH must fail with an error naming the table, the
# offending column and column_ids.json.
$CLIENT --query "DROP TABLE IF EXISTS t_corrupt_mapping SYNC"
$CLIENT --query "
CREATE TABLE t_corrupt_mapping (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;
"

echo "INSERT INTO t_corrupt_mapping VALUES (1, 'x', 1.5)" | $CLIENT

table_dir=$($CLIENT --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 't_corrupt_mapping'")
mapping_file="${table_dir}column_ids.json"

if [ ! -f "${mapping_file}" ]; then
    echo "mapping_file_missing"
    exit 1
fi

$CLIENT --query "DETACH TABLE t_corrupt_mapping SYNC"

# Strip the `c` entry from the mapping (jq-free so the test does not need jq).
python3 - "$mapping_file" <<'PY'
import json
import sys
path = sys.argv[1]
with open(path) as f:
    data = json.load(f)
data['mapping'] = {k: v for k, v in data.get('mapping', {}).items() if k != 'c'}
with open(path, 'w') as f:
    json.dump(data, f)
PY

attach_output=$($CLIENT --query "ATTACH TABLE t_corrupt_mapping" 2>&1 || true)
echo "${attach_output}" | grep -q "Column ID mapping" && echo "error_mentions_mapping" || echo "error_does_not_mention_mapping"
echo "${attach_output}" | grep -q "t_corrupt_mapping" && echo "error_mentions_table" || echo "error_does_not_mention_table"
echo "${attach_output}" | grep -qE "\\bc\\b" && echo "error_mentions_column" || echo "error_does_not_mention_column"
echo "${attach_output}" | grep -q "column_ids.json" && echo "error_mentions_file" || echo "error_does_not_mention_file"

# Restore the `c` entry as identity so the table loads and the test cleans up.
python3 - "$mapping_file" <<'PY'
import json
import sys
path = sys.argv[1]
with open(path) as f:
    data = json.load(f)
data.setdefault('mapping', {})['c'] = 'c'
with open(path, 'w') as f:
    json.dump(data, f)
PY

$CLIENT --query "ATTACH TABLE t_corrupt_mapping"
$CLIENT --query "SELECT a, b, c FROM t_corrupt_mapping ORDER BY a"

$CLIENT --query "DROP TABLE t_corrupt_mapping SYNC"

# Scenario 2: column_ids.json is ENTIRELY absent (deleted out-of-band).  The table
# must refuse to load with an error naming column_ids.json rather than silently
# loading with no mapping and returning defaults for every non-identity column.
$CLIENT --query "DROP TABLE IF EXISTS t_missing_mapping SYNC"
$CLIENT --query "
CREATE TABLE t_missing_mapping (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;
"

# DROP + re-ADD gives `c` the numeric column ID "1": its data lives in `1.bin`
# and is only reachable through column_ids.json.
echo "INSERT INTO t_missing_mapping VALUES (1, 'x', 1.5)" | $CLIENT
$CLIENT --query "ALTER TABLE t_missing_mapping DROP COLUMN c"
$CLIENT --query "ALTER TABLE t_missing_mapping ADD COLUMN c Float64"
echo "INSERT INTO t_missing_mapping VALUES (2, 'y', 9.9)" | $CLIENT

table_dir=$($CLIENT --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 't_missing_mapping'")
mapping_file="${table_dir}column_ids.json"

if [ ! -f "${mapping_file}" ]; then
    echo "mapping_file_missing"
    exit 1
fi

# Keep a copy so the test can clean up after itself regardless of outcome.
backup_file="${CLICKHOUSE_TMP}/04084_column_ids.json"
cp "${mapping_file}" "${backup_file}"

$CLIENT --query "DETACH TABLE t_missing_mapping SYNC"

rm -f "${mapping_file}"

attach_output=$($CLIENT --query "ATTACH TABLE t_missing_mapping" 2>&1 || true)
echo "${attach_output}" | grep -q "column_ids.json" && echo "refused_naming_column_ids_json" || echo "loaded_or_unclear"

# Robust discriminator that does not depend on stderr text: after a refusal
# the table must NOT be present; the buggy silent load leaves it present.
loaded=$($CLIENT --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_missing_mapping'")
echo "table_loaded_after_fileless_attach=${loaded}"

# Restore the mapping and load the table.  On a fixed server the table is still
# detached, so this ATTACH loads it; on a buggy server the earlier ATTACH already
# loaded it (empty mapping) and the SELECT below exposes the silent corruption.
cp "${backup_file}" "${mapping_file}"
$CLIENT --query "ATTACH TABLE t_missing_mapping" 2>/dev/null || true
$CLIENT --query "SELECT a, b, c FROM t_missing_mapping ORDER BY a"

$CLIENT --query "DROP TABLE t_missing_mapping SYNC"
rm -f "${backup_file}"

# Scenario 3: settings-only enablement.  Turning serialization_info_version to
# 'with_column_ids' on an existing table must create column_ids.json in the same
# commit -- committing the setting without a mapping bricks the table at ATTACH.
$CLIENT --query "DROP TABLE IF EXISTS t_settings_enable SYNC"
$CLIENT --query "
CREATE TABLE t_settings_enable (a UInt32, b String)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
"
echo "INSERT INTO t_settings_enable VALUES (1, 'x')" | $CLIENT

# Without the experimental flag the flip must be rejected -- the same gate CREATE
# applies to the version setting alone.
$CLICKHOUSE_CLIENT --query "
ALTER TABLE t_settings_enable MODIFY SETTING serialization_info_version = 'with_column_ids'
" 2>&1 | grep -q "allow_experimental_column_ids" && echo "flip_requires_experimental_flag" || echo "flip_missed_experimental_gate"

# The rejected flip may not have committed anything: the table still reloads.
$CLIENT --query "DETACH TABLE t_settings_enable SYNC"
$CLIENT --query "ATTACH TABLE t_settings_enable"
$CLIENT --query "SELECT a, b FROM t_settings_enable ORDER BY a"

# With the experimental flag the flip commits and creates the mapping in the
# same commit.
$CLIENT --query "
ALTER TABLE t_settings_enable MODIFY SETTING serialization_info_version = 'with_column_ids'
"
table_dir=$($CLIENT --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 't_settings_enable'")
if [ -f "${table_dir}column_ids.json" ]; then
    echo "mapping_created_by_settings_flip"
else
    echo "mapping_missing_after_settings_flip"
fi

$CLIENT --query "DETACH TABLE t_settings_enable SYNC"
$CLIENT --query "ATTACH TABLE t_settings_enable"

# Subsequent DDL works on the activated table: the re-added column gets a numeric ID.
$CLIENT --query "ALTER TABLE t_settings_enable DROP COLUMN b"
$CLIENT --query "ALTER TABLE t_settings_enable ADD COLUMN b String DEFAULT 'def'"
echo "INSERT INTO t_settings_enable VALUES (2, 'y')" | $CLIENT
$CLIENT --query "SELECT a, b FROM t_settings_enable ORDER BY a"
$CLIENT --query "SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_settings_enable' AND active AND column = 'b' AND name LIKE 'all_2%'"

$CLIENT --query "DROP TABLE t_settings_enable SYNC"
