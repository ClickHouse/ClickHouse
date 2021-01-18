#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -n --query "
DROP TABLE IF EXISTS t;
CREATE TABLE t (a LowCardinality(Nullable(String))) ENGINE = Memory;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t FORMAT RawBLOB" < ${BASH_SOURCE[0]}

cat ${BASH_SOURCE[0]} | md5sum
${CLICKHOUSE_CLIENT} -n --query "SELECT * FROM t FORMAT RawBLOB" | md5sum

${CLICKHOUSE_CLIENT} --query "
DROP TABLE t;
"
