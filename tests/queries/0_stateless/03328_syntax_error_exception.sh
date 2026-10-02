#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The reason is in front of the message:
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&max_query_size=20" -d "SELECT 'ыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыы'" | grep -o -F 'ption: Max query size exceeded'

# The message is not too long:
perl -e 'print "SELECT length('"'"'" . ("x" x '262140') . "'"'"')"' | ${CLICKHOUSE_LOCAL} 2>&1 | wc -c | ${CLICKHOUSE_LOCAL} --input-format TSV --query "SELECT c1 < 500 FROM table"

# When it's an unrecognized token, the UTF-8 codepoint is correctly cut:
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -d "SELECT ыыы'" | grep -o -F "Unrecognized token: Syntax error: failed at position 8 (ы): ыыы'"
