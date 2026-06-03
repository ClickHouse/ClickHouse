#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-parallel-replicas, no-object-storage, no-replicated-database, no-shared-merge-tree, no-async-insert
# Regression: when `column_ids.json` is missing an entry for a column that
# still exists in metadata, the table must refuse to load with a clear
# `CORRUPTED_DATA` error rather than silently rebuilding the mapping and
# returning defaults for the affected column.  Auto-rebuild is unsafe
# because DROP + re-ADD makes on-disk files indistinguishable from their
# column ID alone.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1"

$CLIENT --query "DROP TABLE IF EXISTS t_corrupt_mapping SYNC"
$CLIENT --query "
CREATE TABLE t_corrupt_mapping (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;
"

echo "INSERT INTO t_corrupt_mapping VALUES (1, 'x', 1.5)" | $CLIENT

# Locate the table directory and rewrite column_ids.json so that the entry
# for column `c` is missing while metadata.sql still declares `c`.  The
# mapping just keeps the identity entries for `a` and `b` plus the counter.
table_dir=$($CLIENT --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 't_corrupt_mapping'")
mapping_file="${table_dir}column_ids.json"

if [ ! -f "${mapping_file}" ]; then
    echo "mapping_file_missing"
    exit 1
fi

$CLIENT --query "DETACH TABLE t_corrupt_mapping SYNC"

# Strip the `c` entry from the mapping.  Use a small jq-free rewrite so the
# test does not depend on jq being installed.
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

# Attaching must fail loudly.  Capture the error text and check that it
# names the table, the offending column, and column_ids.json.
attach_output=$($CLIENT --query "ATTACH TABLE t_corrupt_mapping" 2>&1 || true)
echo "${attach_output}" | grep -q "Column ID mapping" && echo "error_mentions_mapping" || echo "error_does_not_mention_mapping"
echo "${attach_output}" | grep -q "t_corrupt_mapping" && echo "error_mentions_table" || echo "error_does_not_mention_table"
echo "${attach_output}" | grep -qE "\\bc\\b" && echo "error_mentions_column" || echo "error_does_not_mention_column"
echo "${attach_output}" | grep -q "column_ids.json" && echo "error_mentions_file" || echo "error_does_not_mention_file"

# Restore the mapping so the test cleans up after itself.  Re-insert the
# `c` entry as identity (logical_name == column_id) so the table loads.
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
