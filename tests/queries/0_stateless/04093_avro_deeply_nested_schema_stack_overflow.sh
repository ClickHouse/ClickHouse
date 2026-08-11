#!/usr/bin/env bash
# Tags: no-fasttest, long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Use a pre-generated deeply nested Avro file (2000 levels of nested arrays)
# that would cause stack overflow without proper recursion depth checks.
AVRO_FILE="${CURDIR}/data_avro/deeply_nested_schema.avro"

# Should not crash (SIGSEGV). May succeed or fail with TOO_DEEP_RECURSION depending on build type.
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${AVRO_FILE}', 'Avro') FORMAT Null" > /dev/null 2>&1
[ $? -lt 128 ] || echo "CRASHED"

${CLICKHOUSE_LOCAL} -q "DESC file('${AVRO_FILE}', 'Avro')" > /dev/null 2>&1
[ $? -lt 128 ] || echo "CRASHED"
