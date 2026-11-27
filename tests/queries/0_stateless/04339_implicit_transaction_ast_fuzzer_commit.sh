#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the server-side AST fuzzer (ast_fuzzer_runs)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "create table dedup_test (A Int64) engine = MergeTree order by A"

# Implicit transaction over HTTP (so the commit happens in the streaming executeQuery path) together with the
# server AST fuzzer: the fuzzer resets the transaction stored in the (session) context before running fuzzed
# queries. The implicit transaction commit must run before the fuzzer, otherwise it finds the transaction gone
# and hits a `Logical error: 'txn'`.
SESSION="04339_session_${CLICKHOUSE_DATABASE}_$RANDOM"
# async_insert=0 is required: async inserts are not supported with implicit_transaction, so the INSERT would
# error out before reaching the commit/fuzzer interaction.
URL="${CLICKHOUSE_URL}&session_id=${SESSION}&implicit_transaction=1&async_insert=0&throw_on_unsupported_query_inside_transaction=0&ast_fuzzer_runs=5&ast_fuzzer_any_query=1"
echo -ne '1\n2\n3\n' | ${CLICKHOUSE_CURL} -sS -X POST --data-binary @- "${URL}&query=INSERT+INTO+dedup_test+FORMAT+TSV"

$CLICKHOUSE_CLIENT -q "select count() from dedup_test"
$CLICKHOUSE_CLIENT -q "drop table dedup_test"
