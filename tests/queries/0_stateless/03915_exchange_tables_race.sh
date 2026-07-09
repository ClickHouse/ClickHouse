#!/usr/bin/env bash
# Tags: no-ordinary-database

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The abort this test targets happens in ActionsDAG::updateHeader -> evaluatePartialResult
# (input_rows_count 0/1) during query planning, so it does not need any rows: a concurrent
# EXCHANGE TABLES swapping the column type between analysis and header computation is enough.
# Keep the tables empty - the flaky check reruns this test many times under sanitizers, and
# large Memory-engine inserts multiplied by that repetition exhaust the job's memory/time.

# Base-type drift: swap Float64 with Int256. A strict function (arithmetic) resolved for the
# pre-EXCHANGE type would trip a LOGICAL_ERROR when handed the drifted type during partial
# evaluation, aborting the server in debug/sanitizer builds.
${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS tbl_03007_1;
DROP TABLE IF EXISTS tbl_03007_2;
CREATE TABLE tbl_03007_1 (n Float64) ENGINE=Memory;
CREATE TABLE tbl_03007_2 (n Int256) ENGINE=Memory;
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

# Wrapper-only drift: swap String with Nullable(String). Partial evaluation compares each
# argument type against the type the function was resolved for by exact equality, so a
# wrapper-only drift is do-not-fold too. This covers three otherwise-distinct hazards:
#   - materialize returns its argument type, so under drift it would produce a column whose
#     type differs from the resolved result type (Unexpected-return-type LOGICAL_ERROR);
#   - isNullable is a wrapper-sensitive value folder (folds UInt8(0/1) straight from the
#     argument type), so under drift its result type stays UInt8 and no result-type check
#     would catch a wrong fold: without the exact-type guard it would hand the one-row
#     partial-evaluation callers (JOIN rewrite / shard skipping) a definitive but wrong
#     value instead of "unknown".
${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS tbl_03007_3;
DROP TABLE IF EXISTS tbl_03007_4;
CREATE TABLE tbl_03007_3 (s String) ENGINE=Memory;
CREATE TABLE tbl_03007_4 (s Nullable(String)) ENGINE=Memory;
EOF

for _ in {1..10}; do
    (! ${CLICKHOUSE_CLIENT} --query "SELECT materialize(s) FROM (SELECT * FROM tbl_03007_3)" 2>&1 | grep -E "LOGICAL_ERROR|Unexpected return type") &
    (! ${CLICKHOUSE_CLIENT} --query "SELECT isNullable(s) FROM (SELECT * FROM tbl_03007_3)" 2>&1 | grep -E "LOGICAL_ERROR|Unexpected return type") &
    ${CLICKHOUSE_CLIENT} --query "EXCHANGE TABLES tbl_03007_3 AND tbl_03007_4" 2>/dev/null &
done

wait 2>/dev/null

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS tbl_03007_3;
DROP TABLE IF EXISTS tbl_03007_4;
EOF
