#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Exercise the real terminal-help path from the structured source registration through
# `system.documentation` and `TerminalMarkdownRenderer`. The `Redis` engine has titled Mintlify
# admonitions, so both the label and body must survive while the MDX wrapper disappears.
out=$($CLICKHOUSE_LOCAL -q "help Redis")

printf '%s\n' "$out" | grep -qxF 'Serialization:' \
    || { echo 'Missing titled admonition label'; exit 1; }
echo 'OK: titled admonition label rendered'

printf '%s\n' "$out" | grep -qF 'The primary key will be serialized in binary as a Redis key.' \
    || { echo 'Missing titled admonition body'; exit 1; }
echo 'OK: titled admonition body rendered'

legacy_admonition_re='</?Note([[:space:]][^>]*)?>|:::(note|warning|tip|info|caution|danger|important)'
if printf '%s\n' "$out" | grep -qE "$legacy_admonition_re"; then
    echo 'Raw admonition syntax remains in rendered help'
    exit 1
fi
echo 'OK: raw admonition syntax removed'
