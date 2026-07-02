#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-replicated-database, no-flaky-check, no-sanitizers, no-debug, no-llvm-coverage
# no-fasttest: the `tpch` database is not created in fasttest.
# no-replicated-database: the `tpch` database is not created in DatabaseReplicated mode.
# no-random-settings: these tests verify correctness, not behavior under random settings that may cause memory issues.
# no-flaky-check: TPC-H queries are too expensive for thread fuzzer.
# no-sanitizers, no-debug, no-llvm-coverage: this is one of the heaviest queries. With disk spill
# enabled (see the shared .lib), on slow builds (sanitizers, debug, coverage instrumentation) it
# overruns the test timeout while spilling to disk, or uses too much memory if run in memory. Its
# plan/result correctness is still covered on the fast builds (and, for coverage, in the separate
# `amd_binary_excluded_from_llvm` job that runs no-llvm-coverage tests on a regular binary), so it
# is skipped here.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./04040_tpc_h.lib
. "$CURDIR"/04040_tpc_h.lib

{ echo "USE tpch;"; cat "$CURDIR/../../benchmarks/tpc-h/queries/query_08.sql"; } | $CLICKHOUSE_CLIENT "${SETTINGS[@]}"
