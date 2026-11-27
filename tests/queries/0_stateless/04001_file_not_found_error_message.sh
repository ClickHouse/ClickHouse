#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When the argument to the main binary looks like a file path but doesn't exist,
# the error message should say "no such file" instead of showing the generic help.

# Main binary with non-existent path containing a slash:
${CLICKHOUSE_BINARY} /tmp/nonexistent_04001.sql 2>&1
echo "exit: $?"

# Main binary with non-existent path containing a dot:
${CLICKHOUSE_BINARY} nonexistent_04001.sql 2>&1
echo "exit: $?"

# clickhouse-local with non-existent file path:
${CLICKHOUSE_LOCAL} /tmp/nonexistent_04001.sql 2>&1 | grep -o 'No such file: /tmp/nonexistent_04001.sql'
${CLICKHOUSE_LOCAL} nonexistent_04001.rep 2>&1 | grep -o 'No such file: nonexistent_04001.rep'

# A bare word without dots or slashes should still show the generic help:
${CLICKHOUSE_BINARY} nonexistent_command 2>&1 | grep -F 'Use one of the following commands'
