#!/usr/bin/env bash

# The errors below are counted on stderr, where the server log of the failing query would land
# as well and be counted a second time.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A batch run asked to `--ignore-error` reports its failed statements and exits with a success
# code. The `ignore_error` flag alone does not say who asked for that, though: `processOptions`
# turns it on for the fuzzing modes too, so that they tolerate unparseable input. Those keep their
# error exit code, because the AST fuzzer reports an early stop - losing the server, say - through
# nothing else, and a truncated run would otherwise be reported as a clean one.
#
# `--create-query-fuzzer-runs` is the mode that can be pinned down without taking the server away
# mid-run: it turns `ignore_error` on but never enters the fuzz loop, which is what discards the
# error of every step, so the exception of the last statement reaches `Client::main` just like in
# a plain batch.

ERROR='FUNCTION_THROW_IF_VALUE_IS_NON_ZERO'

OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.out"
ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"
trap 'rm -f "$OUT" "$ERR"' EXIT

report()
{
    local exit_code="$1"

    local outcome='success'
    [ "$exit_code" -eq 0 ] || outcome='failure'

    echo "stdout=[$(tr '\n' ' ' < "$OUT" | sed -e 's/[[:space:]]*$//')]" \
         "reported errors=$(grep -c -F "$ERROR" "$ERR") exit=${outcome}"
}

echo '--- asking for --ignore-error gets a success exit'
${CLICKHOUSE_CLIENT} --ignore-error --query "SELECT 'ok'; SELECT throwIf(1);" > "$OUT" 2> "$ERR"
report "$?"

echo '--- a mode that turns ignore_error on itself keeps the error exit'
${CLICKHOUSE_CLIENT} --create-query-fuzzer-runs=1 --query "SELECT 'ok'; SELECT throwIf(1);" > "$OUT" 2> "$ERR"
report "$?"

echo '--- and it still carries on to the statement after the failure'
${CLICKHOUSE_CLIENT} --create-query-fuzzer-runs=1 --query "SELECT throwIf(1); SELECT 'carried on';" > "$OUT" 2> "$ERR"
report "$?"
