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
-- Insert rows so the SELECT reads non-empty chunks: this exercises the runtime
-- ExpressionActions execution path (function->execute on real data), not only the
-- header/partial-result path. A running SELECT keeps the storage snapshot resolved at
-- analysis time, so EXCHANGE cannot feed the function a drifted type at runtime; once the
-- exchange settles, a fresh query re-resolves and reports a clean ILLEGAL_TYPE_OF_ARGUMENT.
INSERT INTO tbl_03007_1 SELECT number * 0.5 FROM numbers(100000);
INSERT INTO tbl_03007_2 SELECT number FROM numbers(100000);
EOF

for _ in {1..10}; do
    (! ${CLICKHOUSE_CLIENT} --query "SELECT n * 0.123 FROM (SELECT * FROM tbl_03007_1)" 2>&1 | grep LOGICAL_ERROR) &
    ${CLICKHOUSE_CLIENT} --query "EXCHANGE TABLES tbl_03007_1 AND tbl_03007_2" 2>/dev/null &
done

wait 2>/dev/null

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS tbl_03007_1;
DROP TABLE IF EXISTS tbl_03007_2;
EOF
