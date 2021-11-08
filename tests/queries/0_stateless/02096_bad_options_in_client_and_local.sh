#!/usr/bin/env bash
# shellcheck disable=SC2206

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --unknown-option 2>&1 | grep -F -q "UNRECOGNIZED_ARGUMENTS" && echo "OK" || echo "FAIL"

${CLICKHOUSE_LOCAL} --unknown-option-1 --unknown-option-2 2>&1 | grep -F -q "UNRECOGNIZED_ARGUMENTS" && echo "OK" || echo "FAIL"

${CLICKHOUSE_LOCAL} -- --unknown-option 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo "OK" || echo "FAIL"

${CLICKHOUSE_LOCAL} -- 'positional-argument' 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo "OK" || echo "FAIL"

${CLICKHOUSE_LOCAL} -f 2>&1 | grep -F -q "Bad arguments" && echo "OK" || echo "FAIL"

${CLICKHOUSE_LOCAL} --query 2>&1 | grep -F -q "Bad arguments" && echo "OK" || echo "FAIL"


${CLICKHOUSE_CLIENT} --unknown-option 2>&1 | grep -F -q "UNRECOGNIZED_ARGUMENTS" && echo "OK" || echo "FAIL"

${CLICKHOUSE_CLIENT} --unknown-option-1 --unknown-option-2 2>&1 | grep -F -q "UNRECOGNIZED_ARGUMENTS" && echo "OK" || echo "FAIL"

${CLICKHOUSE_CLIENT} -- --unknown-option 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo "OK" || echo "FAIL"

${CLICKHOUSE_CLIENT} -- 'positional-argument' 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo "OK" || echo "FAIL"

${CLICKHOUSE_CLIENT} --j 2>&1 | grep -F -q "Bad arguments" && echo "OK" || echo "FAIL"

${CLICKHOUSE_CLIENT} --query 2>&1 | grep -F -q "Bad arguments" && echo "OK" || echo "FAIL"



