#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: polyglot requires Rust build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

POLY="--allow_experimental_polyglot_dialect 1 --dialect polyglot --polyglot_dialect postgresql"

# A foreign-dialect query is normally sent to the server verbatim, with the parse-time `dialect`
# pinned, so the server transpiles exactly the text the client classified. `--allow_merge_tree_settings`
# breaks that assumption: to add the MergeTree settings given on the command line, the client serializes
# the parsed AST back to SQL — and for a foreign dialect that AST is the transpiled one. The outbound
# text is then ClickHouse SQL, so the client pins `dialect` to `clickhouse` for it; otherwise the server
# would transpile it a second time, which silently dropped the added settings.
$CLICKHOUSE_CLIENT $POLY --allow_merge_tree_settings --index_granularity=1024 -q "CREATE TABLE t (x int, y text)"
$CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE t" | grep -o 'index_granularity = 1024'

# The transpiled types survive the rewrite.
$CLICKHOUSE_CLIENT -q "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't' ORDER BY name"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
