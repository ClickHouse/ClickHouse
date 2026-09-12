#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `--ignore-error` reports a failed statement and carries on with the next one.
#
# `LocalServer::processError` used to return early under `--ignore-error`, so `clickhouse-local`
# printed nothing at all for a statement that failed at execution and a broken script was
# indistinguishable from a working one. Parse errors were reported even then, because those are
# printed by `parseQuery` and never reach `processError`.
#
# The two programs also disagreed on the exit code: `clickhouse-client` returned the code of
# whichever error the last statement happened to hit, which says nothing about the rest of the
# batch, while `clickhouse-local` returned success. Both report success now.

# The errors are counted by name instead of compared verbatim, because the message carries a
# version and, for the client, the endpoint the exception came from.
ERROR='FUNCTION_THROW_IF_VALUE_IS_NON_ZERO'

FAIL_FIRST="SELECT throwIf(1); SELECT 'still runs';"
FAIL_LAST="SELECT 'still runs'; SELECT throwIf(1);"

OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.out"
ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"
trap 'rm -f "$OUT" "$ERR"' EXIT

report()
{
    local label="$1"
    local exit_code="$2"

    # Only the presence of a failure is reported, not the code, which is the exception code
    # truncated to the range an exit code can hold.
    local outcome='success'
    [ "$exit_code" -eq 0 ] || outcome='failure'

    echo "${label}: stdout=[$(tr '\n' ' ' < "$OUT" | sed -e 's/[[:space:]]*$//')]" \
         "reported errors=$(grep -c -F "$ERROR" "$ERR") exit=${outcome}"
}

echo '--- a failure followed by a working statement, with --ignore-error'
${CLICKHOUSE_LOCAL} --ignore-error --query "$FAIL_FIRST" > "$OUT" 2> "$ERR"
report 'local ' "$?"
${CLICKHOUSE_CLIENT} --ignore-error --query "$FAIL_FIRST" > "$OUT" 2> "$ERR"
report 'client' "$?"

echo '--- the failure as the last statement, with --ignore-error'
${CLICKHOUSE_LOCAL} --ignore-error --query "$FAIL_LAST" > "$OUT" 2> "$ERR"
report 'local ' "$?"
${CLICKHOUSE_CLIENT} --ignore-error --query "$FAIL_LAST" > "$OUT" 2> "$ERR"
report 'client' "$?"

echo '--- without --ignore-error the run stops at the failure and fails'
${CLICKHOUSE_LOCAL} --query "$FAIL_FIRST" > "$OUT" 2> "$ERR"
report 'local ' "$?"
${CLICKHOUSE_CLIENT} --query "$FAIL_FIRST" > "$OUT" 2> "$ERR"
report 'client' "$?"
