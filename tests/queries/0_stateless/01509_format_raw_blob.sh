#!/usr/bin/env bash
# Tags: no-parallel-replicas

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS t;
CREATE TABLE t (a LowCardinality(Nullable(String))) ENGINE = Memory;
"
LOREM_IPSUM=$(cat <<EOF
Lorem ipsum dolor sit amet, consectetur adipiscing elit.
Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.
Nisi ut aliquip ex ea commodo consequat.
EOF
)

echo "$LOREM_IPSUM" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO t FORMAT RawBLOB"

echo "$LOREM_IPSUM" | md5sum
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t FORMAT RawBLOB" | md5sum

${CLICKHOUSE_CLIENT} --query "
DROP TABLE t;
"
