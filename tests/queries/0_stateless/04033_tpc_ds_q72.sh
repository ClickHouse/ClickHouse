#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-replicated-database, no-flaky-check, no-sanitizers, no-debug, no-llvm-coverage
# no-fasttest: TPC-DS tables use web disk (S3) which is not available in fasttest.
# no-random-settings: random session_timezone, query_plan_join_swap_table, etc. change query results.
# no-replicated-database: the `tpcds` database is not created in DatabaseReplicated mode.
# no-flaky-check: TPC-DS queries are too expensive for thread fuzzer.
# no-sanitizers, no-debug, no-llvm-coverage: this is one of the heaviest queries. With disk spill
# enabled (see the shared .lib), on slow builds (sanitizers, debug, coverage instrumentation) it
# overruns the test timeout while spilling to disk, or uses too much memory if run in memory. Its
# plan/result correctness is still covered on the fast builds (and, for coverage, in the separate
# `amd_binary_excluded_from_llvm` job that runs no-llvm-coverage tests on a regular binary), so it
# is skipped here.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./04033_tpc_ds.lib
. "$CURDIR"/04033_tpc_ds.lib

{ echo "USE tpcds;"; cat "$CURDIR/../../benchmarks/tpc-ds/queries/query_72.sql"; } | $CLICKHOUSE_CLIENT "${SETTINGS[@]}"
