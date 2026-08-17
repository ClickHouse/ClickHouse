#!/usr/bin/env bash
# Tags: no-ordinary-database

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS tbl_03007_1;
DROP TABLE IF EXISTS tbl_03007_2;
CREATE TABLE tbl_03007_1 (n Float64) ENGINE=Memory;
CREATE TABLE tbl_03007_2 (n Int256) ENGINE=Memory;
EOF

for _ in {1..10}; do
    (! ${CLICKHOUSE_CLIENT} --query "SELECT n * 0.123 FROM (SELECT * FROM tbl_03007_1)" 2>&1 | grep LOGICAL_ERROR) &
    ${CLICKHOUSE_CLIENT} --query "EXCHANGE TABLES tbl_03007_1 AND tbl_03007_2" &
done

wait

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS tbl_03007_1;
DROP TABLE IF EXISTS tbl_03007_2;
EOF
