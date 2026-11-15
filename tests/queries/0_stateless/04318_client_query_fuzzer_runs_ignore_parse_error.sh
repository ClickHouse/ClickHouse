#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `--query-fuzzer-runs` auto-enables `ignore-error` inside the client. When a query in the
# input file fails to parse (the client throws BAD_ARGUMENTS / SYNTAX_ERROR before reaching
# the server), `analyzeMultiQueryText` skips past the failing query and returns
# `CONTINUE_PARSING`. The matching `client_exception` must be cleared at that skip;
# otherwise it lingers and `Client::main` returns its code as the process exit code, even
# though the run was asked to ignore parse errors. This regression test exercises that
# path through the AST fuzzer entrypoint that the targeted CI job uses.

QUERIES_FILE=$(mktemp -p "${CLICKHOUSE_TMP:-/tmp}" "ch_${CLICKHOUSE_TEST_UNIQUE_NAME:-04318}.XXXXXX.sql")
trap 'rm -f "$QUERIES_FILE"' EXIT

cat > "$QUERIES_FILE" <<'SQL'
SELECT 1;
CREATE TABLE t (a Int32 PRIMARY KEY, b Int32) PRIMARY KEY a ORDER BY a;
SQL

# Scenario A: pure parse failure under `--query-fuzzer-runs` (which auto-enables
# `ignore-error`). Pre-fix: exits with `BAD_ARGUMENTS` because `client_exception`
# remained pinned across the fuzzer's `CONTINUE_PARSING` skip.
# Stdout output is non-deterministic in fuzzer mode (per-iteration "Dump of fuzzed AST:"),
# so the regression assertion is on the exit code only.
${CLICKHOUSE_CLIENT} --query-fuzzer-runs=2 --create-query-fuzzer-runs=2 --queries-file "$QUERIES_FILE" > /dev/null 2>&1
echo "exit: $?"

# Scenario B: a server-error statement followed by a parse-error statement under
# `--ignore-error`. Pre-fix the parse-skip cleared only `client_exception`, leaving the
# `server_exception` from statement 1 pinned. `Client::main` prefers `server_exception`
# over `client_exception` for the exit code, so the process exited with the stale server
# error code (e.g. UNKNOWN_TABLE / UNKNOWN_FUNCTION) even though `--ignore-error` had
# elected to continue. Post-fix: the skip mirrors the per-query reset and clears all four
# error fields, so the exit code reflects only the rest of the run (clean here).
${CLICKHOUSE_CLIENT} --multiquery --ignore-error --query \
"SELECT * FROM nonexistent_table_for_04318; CREATE TABLE t (a Int32 PRIMARY KEY, b Int32) PRIMARY KEY a ORDER BY a;" \
> /dev/null 2>&1
echo "exit: $?"
