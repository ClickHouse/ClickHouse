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

# Run with both analyzers: the LOGICAL_ERROR from a table exchanged mid-query
# only reproduces on the old analyzer, so pinning `enable_analyzer` here exercises
# the fixed path deterministically on any config instead of relying on which
# analyzer the CI shard happens to default to.
# Each iteration races one SELECT against one EXCHANGE and waits for both:
# at most 2 concurrent clients per test instance. Batching all 20 pairs at
# once OOMs the flaky check, which runs 8 instances in parallel and each
# sanitizer-instrumented client takes ~0.5 GB.
for enable_analyzer in 0 1; do
    for _ in {1..10}; do
        (! ${CLICKHOUSE_CLIENT} --enable_analyzer="$enable_analyzer" --query "SELECT n * 0.123 FROM (SELECT * FROM tbl_03007_1)" 2>&1 | grep LOGICAL_ERROR) &
        ${CLICKHOUSE_CLIENT} --query "EXCHANGE TABLES tbl_03007_1 AND tbl_03007_2" 2>/dev/null &
        wait 2>/dev/null
    done
done

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS tbl_03007_1;
DROP TABLE IF EXISTS tbl_03007_2;
EOF
