#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: polyglot requires Rust build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A foreign-dialect query is normally sent to the server verbatim, with the parse-time `dialect`
# pinned, so the server transpiles exactly the text the client classified. `--allow_merge_tree_settings`
# breaks that assumption: to add the MergeTree settings given on the command line, the client serializes
# the parsed AST back to SQL — and for a foreign dialect that AST is the transpiled one. The outbound
# text is then ClickHouse SQL, so the client pins `dialect` to `clickhouse` for it; otherwise the server
# would transpile it a second time.
#
# The rewrite only fires when the parsed AST carries an explicit MergeTree engine, and only the
# `clickhouse` source dialect can express one: the other bundled dialects either reject the `ENGINE`
# clause or drop it in transpilation, leaving a `CREATE` with no storage definition — and a `CREATE`
# relying on the default table engine ignores the command-line MergeTree settings in the native dialect
# too (see `addMergeTreeSettings`), so polyglot behaves the same there.

POLY_CH="--allow_experimental_polyglot_dialect 1 --dialect polyglot --polyglot_dialect clickhouse"

query_id="04844_${CLICKHOUSE_DATABASE}_${RANDOM}${RANDOM}"
$CLICKHOUSE_CLIENT $POLY_CH --allow_merge_tree_settings --index_granularity=1024 --query_id "$query_id" -q "CREATE TABLE t (x Int32, y String) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE t" | grep -o 'index_granularity = 1024'

# The wire carried the rewritten text (with the added setting), and it was sent as ClickHouse SQL —
# the pinned `dialect` must not still ask the server to transpile it a second time. A query_log
# entry can be written after the client has already received the response, so retry the flush until
# the entry shows up.
for _ in {1..100}
do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    seen=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'")
    [ "$seen" = "1" ] && break
    sleep 0.3
done
$CLICKHOUSE_CLIENT -q "SELECT if(Settings['dialect'] = 'polyglot', 'retranspiled', 'sent as SQL'), query LIKE '%SETTINGS index_granularity = 1024%' FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"

# In a foreign SQL dialect the transpiled `CREATE` has no MergeTree engine, so — exactly like a native
# `CREATE` relying on the default table engine — the command-line MergeTree settings do not apply, and
# the query is sent verbatim. The transpiled types still work.
POLY_PG="--allow_experimental_polyglot_dialect 1 --dialect polyglot --polyglot_dialect postgresql"
$CLICKHOUSE_CLIENT $POLY_PG --allow_merge_tree_settings --index_granularity=1024 -q "CREATE TABLE t (x int, y text)"
$CLICKHOUSE_CLIENT -q "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't' ORDER BY name"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
