#!/usr/bin/env bash
# Tags: no-ordinary-database

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Companion to 03915_exchange_tables_race: same read-consistency guarantee of the per-query storage
# cache (`Context::getOrCacheStorage`), but reached through a plain RENAME swap instead of EXCHANGE.
# The PR that fixed this (#110048) restored pinning for a name that is "renamed OR exchanged"
# mid-query; 03915 only exercises the exchange path. A 3-way RENAME (a->tmp, b->a, tmp->b) reassigns
# the name `a` to a different UUID with a different column type, so a running SELECT that pinned `a`
# as Float64 and re-resolved it as Int256 would be analyzed for one type and executed against
# another, raising a `LOGICAL_ERROR`. The pinned entry must keep the SELECT reading the version it
# was planned against.

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS tbl_03971_a;
DROP TABLE IF EXISTS tbl_03971_b;
DROP TABLE IF EXISTS tbl_03971_tmp;
CREATE TABLE tbl_03971_a (n Float64) ENGINE=Memory;
CREATE TABLE tbl_03971_b (n Int256) ENGINE=Memory;
EOF

# Run with both analyzers: the LOGICAL_ERROR from a table swapped mid-query only reproduces on the
# old analyzer, so pinning `enable_analyzer` here exercises the fixed path deterministically on any
# config instead of relying on which analyzer the CI shard happens to default to.
# Each iteration races one SELECT against one RENAME and waits for both: at most 2 concurrent
# clients per test instance, so the flaky check (8 instances in parallel, each sanitizer client
# ~0.5 GB) stays well under the container memory limit.
for enable_analyzer in 0 1; do
    for _ in {1..10}; do
        (! ${CLICKHOUSE_CLIENT} --enable_analyzer="$enable_analyzer" --query "SELECT n * 0.123 FROM (SELECT * FROM tbl_03971_a)" 2>&1 | grep LOGICAL_ERROR) &
        select_pid=$!
        ${CLICKHOUSE_CLIENT} --query "RENAME TABLE tbl_03971_a TO tbl_03971_tmp, tbl_03971_b TO tbl_03971_a, tbl_03971_tmp TO tbl_03971_b" 2>/dev/null &
        rename_pid=$!
        # Wait on each PID explicitly: a bare `wait` returns 0 regardless of child status, so
        # set -e would never see a LOGICAL_ERROR from the SELECT branch or a failed RENAME.
        wait "$select_pid"
        wait "$rename_pid"
    done
done

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS tbl_03971_a;
DROP TABLE IF EXISTS tbl_03971_b;
DROP TABLE IF EXISTS tbl_03971_tmp;
EOF
