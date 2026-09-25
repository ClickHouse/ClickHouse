#!/usr/bin/env bash
# Tags: long, no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database, no-shared-merge-tree
# no-parallel: a merge queued behind the merges of other tests is not in system.merges yet.
# no-random-settings, no-random-merge-tree-settings: random max_threads and merge_max_block_size can stretch the 60M-row replay past the timeout.
# no-replicated-database, no-shared-merge-tree: the replay must create the plain ReplacingMergeTree table the perf test measures.

# `perf.py --stop-merges` stops merges right after the setup of the `read_in_reverse_order_final` perf test,
# so no merge of its table may be running there: the stop would freeze a mid-merge part layout.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS read_in_reverse_order_final"

# The setup queries as `perf.py` runs them (none if the test keeps merges running), then, in the same session,
# the merges its stop would cut off.
{
    python3 - "$CURDIR/../../performance/read_in_reverse_order_final.xml" <<'EOF'
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()
if root.get("keep_merges_running", "0") in ("0", "false", ""):
    for e in root:
        if e.tag in ("create_query", "fill_query"):
            print(e.text.strip().rstrip(";") + ";")
EOF
    echo "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 'read_in_reverse_order_final';"
} | $CLICKHOUSE_CLIENT --max_rows_to_read 0

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS read_in_reverse_order_final"
