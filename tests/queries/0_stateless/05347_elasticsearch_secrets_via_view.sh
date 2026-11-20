#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# A user who may read a view must not learn the credentials the view was created with.
# The storage opens no connection at creation time, so the view exists without an Elasticsearch server.

secret="secret_value_do_not_log"
reader="reader_${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $reader"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS v_es"
$CLICKHOUSE_CLIENT --enable_elasticsearch 1 -q "CREATE VIEW v_es DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT * FROM elasticsearch('http://localhost:1', 'idx', 'u', '$secret')"
$CLICKHOUSE_CLIENT -q "CREATE USER $reader IDENTIFIED WITH plaintext_password BY 'reader_password'"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_es TO $reader"
trap '$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $reader"; $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS v_es"' EXIT

function as_reader()
{
    $CLICKHOUSE_CLIENT --user "$reader" --password reader_password --max_threads 1 --enable_elasticsearch 1 -q "$1" 2>&1 || true
}

function report()
{
    local label=$1
    local output=$2
    local expect_hidden=$3
    local secret_count
    secret_count=$(echo "$output" | grep -c "$secret" || true)
    if [ "$expect_hidden" = "1" ]; then
        local hidden_count
        hidden_count=$(echo "$output" | grep -c -F '[HIDDEN]' || true)
        echo "$label secret=$secret_count hidden=$((hidden_count > 0))"
    else
        echo "$label secret=$secret_count"
    fi
}

report "show_create"   "$(as_reader "SHOW CREATE VIEW v_es")" 1
report "system_tables" "$(as_reader "SELECT create_table_query, as_select FROM system.tables WHERE database = currentDatabase() AND name = 'v_es'")" 1
# A view with secrets is not expanded for a reader without the displaySecretsInShowAndSelect privilege:
# the EXPLAIN flavours show the view name or a sealed plan, so only the absence of the secret is checked.
report "explain_ast"   "$(as_reader "EXPLAIN AST SELECT * FROM v_es")" 0
report "explain_syntax" "$(as_reader "EXPLAIN SYNTAX SELECT * FROM v_es")" 0
report "explain_query_tree" "$(as_reader "EXPLAIN QUERY TREE SELECT * FROM v_es")" 0
report "explain_actions" "$(as_reader "EXPLAIN actions = 1 SELECT * FROM v_es")" 0
report "explain_pipeline" "$(as_reader "EXPLAIN PIPELINE SELECT * FROM v_es")" 0
report "select_error" "$(as_reader "SELECT * FROM v_es SETTINGS http_max_tries = 1")" 0
