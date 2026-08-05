#!/usr/bin/env bash
# Tags: no-llvm-coverage
# - no-llvm-coverage: flaky under coverage instrumentation. The internal 10 s
#   `connect_timeout` / `handshake_timeout` in `clickhouse-benchmark` (see
#   `DBMS_DEFAULT_CONNECT_TIMEOUT_SEC` and `handshake_timeout_ms`) can fire
#   under the slowdown of LLVM source-based coverage + parallel test load.
#   The resulting `Code: 209. DB::NetException: Timeout exceeded ... (SOCKET_TIMEOUT)`
#   is not in the `grep -v` filter list and bleeds into stdout, tripping the
#   "exception in stdout" detector. Same pattern fixed for
#   `02550_benchmark_connections_credentials` (PR #103298).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# No additional stack traces or errors other than invertly matched ones should be found.
$CLICKHOUSE_BENCHMARK --bad-option |& grep -v "DB::Exception: Unrecognized option '--bad-option'"
$CLICKHOUSE_BENCHMARK --timelimit "bad value" |& grep -v "Bad arguments: the argument ('bad value') for option '--timelimit' is invalid"
$CLICKHOUSE_BENCHMARK --user "invalid user" --query "SELECT 1" |& grep -v -e "DB::Exception: invalid user: Authentication failed" -e "Loaded"
$CLICKHOUSE_BENCHMARK pos_arg --query "SELECT 1" |& grep -v "DB::Exception: Positional option 'pos_arg' is not supported."
$CLICKHOUSE_BENCHMARK -i 1 --query "SELECT 1" -- --function_sleep_max_microseconds_per_block=3000000000 |& grep -e "Exception" -e "Loaded"

exit 0