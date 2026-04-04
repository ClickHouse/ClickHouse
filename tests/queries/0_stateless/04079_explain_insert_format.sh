#!/usr/bin/env bash
# Test that FORMAT applies to EXPLAIN output, not to the inner INSERT query.
# https://github.com/ClickHouse/ClickHouse/issues/67321

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Single FORMAT: should apply to EXPLAIN, not to INSERT.
# FORMAT JSONEachRow wraps the output in JSON; if FORMAT were consumed by INSERT,
# the output would be plain text without JSON structure.
echo "--- single FORMAT ---"
${CLICKHOUSE_CLIENT} --query "EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT 1 FORMAT JSONEachRow" | grep -c '"explain"'

# Double FORMAT: first FORMAT goes to INSERT, second to EXPLAIN.
echo "--- double FORMAT ---"
${CLICKHOUSE_CLIENT} --query "EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT 1 FORMAT CSV FORMAT JSONEachRow" | grep -c '"explain"'
