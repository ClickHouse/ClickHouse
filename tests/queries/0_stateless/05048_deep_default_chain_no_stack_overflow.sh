#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TAIL=$(printf ' + rand()%.0s' {1..100})
COLUMNS="c0 UInt64 DEFAULT 1"
for i in {1..99}; do
    COLUMNS="$COLUMNS, c$i UInt64 DEFAULT c$((i - 1))$TAIL"
done

# Accepting this chain and rejecting it with TOO_DEEP_RECURSION are both correct, and which one
# happens depends on the frame size of the build, so both print the same token. Anything else - a
# parser or AST limit, a memory limit, a dead server - means the statement never reached the deep
# traversal, and is printed verbatim so that it cannot match the reference.
OUT=$(${CLICKHOUSE_CLIENT} --enable_analyzer=1 --query "CREATE TABLE chain ($COLUMNS) ENGINE = Memory" 2>&1)
if [[ -z "$OUT" || "$OUT" == *TOO_DEEP_RECURSION* ]]; then
    echo "created or TOO_DEEP_RECURSION"
else
    echo "unexpected outcome: $OUT"
fi

${CLICKHOUSE_CLIENT} --query "SELECT 'alive'"
