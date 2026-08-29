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

# Accepting this chain and rejecting it with TOO_DEEP_RECURSION are both correct; the depth at
# which the switch happens depends on the frame size of the build. A parser or AST limit, on the
# other hand, means the statement no longer reaches the deep traversal at all.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE chain ($COLUMNS) ENGINE = Memory" 2>&1 \
    | grep -cE 'TOO_BIG_AST|TOO_SLOW_PARSING|SYNTAX_ERROR' ||:

${CLICKHOUSE_CLIENT} --query "SELECT 'alive'"
