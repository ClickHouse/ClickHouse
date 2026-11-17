#!/usr/bin/env bash

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