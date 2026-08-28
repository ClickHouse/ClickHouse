#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The message comes from the setting's field, which does not know its own name, so a value of the wrong
# type or out of range used to be reported without saying which setting was being set.
run()
{
    echo "--- $1"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "$1" 2>&1 \
        | grep -m1 -oE 'Code: [0-9]+\. DB::Exception: .*' \
        | sed -e 's/^Code: \([0-9]*\)\. DB::Exception: /Code: \1. /' \
              -e 's/: While executing .*//' -e 's/ (version [^)]*)$//'
}

echo '=== a value of the wrong type or out of range names the setting and the value'
run "SELECT 1 SETTINGS max_threads = 'abc'"
run "SELECT 1 SETTINGS max_threads = -1"
run "SELECT 1 SETTINGS max_block_size = 0"
run "SELECT 1 SETTINGS max_memory_usage = '10 elephants'"
run "SET max_threads = 'abc'"

echo
echo '=== an unknown setting keeps the message it had, with no context appended'
# The text of that message depends on whether custom-setting prefixes are configured, so assert the two
# things that matter: the hint survives, and no ": while setting ..." is appended to it.
unknown=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "SELECT 1 SETTINGS max_threadz = 4" 2>&1)
echo "code: $(echo "$unknown" | grep -oE 'UNKNOWN_SETTING' | head -1)"
echo "hint: $(echo "$unknown" | grep -oE "Maybe you meant \['max_threads'\]" | head -1)"
echo "context appended: $(echo "$unknown" | grep -c 'while setting')"

echo
echo '=== values that are fine still work'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "SELECT 1 SETTINGS max_threads = 4"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "SELECT 1 SETTINGS max_memory_usage = '1G'"
