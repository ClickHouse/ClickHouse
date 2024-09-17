#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test (s String) ENGINE = Memory"

# Calling an unknown function should not lead to creation of a 'user_defined' directory in the current directory
${CLICKHOUSE_CLIENT} --query "SET allow_experimental_analyzer = 0;INSERT INTO test VALUES (xyz('abc'))" 2>&1 | grep -o -F 'Unknown function'
${CLICKHOUSE_CLIENT} --query "SET allow_experimental_analyzer = 1;INSERT INTO test VALUES (xyz('abc'))" 2>&1 | grep -o -F "Function with name 'xyz' does not exist."

ls -ld user_defined 2> /dev/null

${CLICKHOUSE_CLIENT} --query "DROP TABLE test"
