#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS t;
CREATE TABLE t (a LowCardinality(Nullable(String))) ENGINE = Memory;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t FORMAT RawBLOB" < ${BASH_SOURCE[0]}

cat ${BASH_SOURCE[0]} | md5sum
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t FORMAT RawBLOB" | md5sum

${CLICKHOUSE_CLIENT} --query "
DROP TABLE t;
"
