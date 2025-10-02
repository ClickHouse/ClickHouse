#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# No additional stack traces or errors other than invertly matched ones should be found.
$CLICKHOUSE_BENCHMARK --bad-option |& grep -v "DB::Exception: Unrecognized option '--bad-option'"
$CLICKHOUSE_BENCHMARK --timelimit "bad value" |& grep -v "Bad arguments: the argument ('bad value') for option '--timelimit' is invalid"
$CLICKHOUSE_BENCHMARK --user "invalid user" --query "SELECT 1" |& grep -v -e "DB::Exception: invalid user: Authentication failed" -e "Loaded 1 queries"

exit 0