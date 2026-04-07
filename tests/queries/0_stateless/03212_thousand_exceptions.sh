#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# This should not be too slow, even under sanitizers.
(echo "SYSTEM STOP THREAD FUZZER;"; yes "SELECT throwIf(1); SELECT '.' FORMAT Values;") | head -n 1001 | $CLICKHOUSE_LOCAL --ignore-error 2>/dev/null
