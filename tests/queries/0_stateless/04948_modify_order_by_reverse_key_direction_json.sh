#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: replicated DDL stores the entry as SQL text, which cannot carry a
# sorting key direction, so the JSON AST does not survive the round trip.

# `ALTER TABLE ... MODIFY ORDER BY` must not change the sort direction of a retained sorting
# key column in EITHER direction. The `ALTER` parser cannot express `ASC`/`DESC`, but the
# `clickhouse_json` dialect submits an AST directly, so it can hand `MODIFY ORDER BY` a
# `StorageOrderByElement` with `direction = -1` and turn an ascending key descending. The parts
# on disk stay physically ascending, so index analysis would prune the wrong marks.

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

T="t_json_dir_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $T"
$CLICKHOUSE_CLIENT --query "CREATE TABLE $T (a UInt64, v String) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 128"
$CLICKHOUSE_CLIENT --query "INSERT INTO $T SELECT number, 'foo' FROM numbers(1000)"

# Build the ALTER's JSON AST from the server, then splice in a descending `order_by` node
# harvested from a CREATE (the ALTER parser cannot produce one).
ALTER_JSON=$($CLICKHOUSE_CLIENT --query "SELECT parseQueryToJSON('ALTER TABLE $T MODIFY ORDER BY a')")
DESC_NODE=$($CLICKHOUSE_CLIENT --query "SELECT parseQueryToJSON('CREATE TABLE x (a UInt64) ENGINE = MergeTree ORDER BY a DESC')" | jq -c '.storage.order_by')
SPLICED=$(jq -c --argjson node "$DESC_NODE" '.command_list.children[0].order_by = $node' <<< "$ALTER_JSON")

echo "=== the spliced node carries direction = -1 ==="
jq -r '.command_list.children[0].order_by | "\(.type) direction=\(.direction)"' <<< "$SPLICED"

echo "=== turning an ascending key descending is refused ==="
# Report a derived token rather than the raw output, so an accepting server produces a readable
# diff instead of an empty one.
OUT=$($CLICKHOUSE_CLIENT --enable_json_ast_dialect=1 --dialect=clickhouse_json --query="$SPLICED" 2>&1 || true)
if grep -qF 'Sort direction of the sorting key column' <<< "$OUT"; then
    grep -oE 'Code: [0-9]+|BAD_ARGUMENTS' <<< "$OUT" | head -2
    echo "refused"
else
    echo "NOT REFUSED: $OUT"
fi

echo "=== the key is still ascending and reads stay correct ==="
# 1 would mean the key went descending. `sorting_key` renders without the direction, so the
# check has to look at `create_table_query`.
$CLICKHOUSE_CLIENT --query "SELECT position(create_table_query, 'ORDER BY a DESC') > 0 FROM system.tables WHERE database = currentDatabase() AND name = '$T'"
$CLICKHOUSE_CLIENT --query "SELECT sum(a) FROM $T WHERE a >= 500 SETTINGS use_lightweight_primary_key_index_analysis = 0"
$CLICKHOUSE_CLIENT --query "SELECT sum(a) FROM $T WHERE a >= 500 SETTINGS use_lightweight_primary_key_index_analysis = 1"

echo "=== an ascending MODIFY ORDER BY through the same dialect is still accepted ==="
$CLICKHOUSE_CLIENT --enable_json_ast_dialect=1 --dialect=clickhouse_json --query="$ALTER_JSON"
echo "accepted"

$CLICKHOUSE_CLIENT --query "DROP TABLE $T"
