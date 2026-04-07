#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# We only care about does it crashes or not with:
#
#   __cxa_guard_acquire detected recursive initialization: do you have a function-local static variable whose initialization depends on that function
#
# Due to recursive initialization of SymbolIndex, one on fly due to Exception,
# another one due to allocation for SymbolIndex itself, that exceed default
# limit of 16MiB for non-tracked allocations
#
# NOTE: --log-level=test is important (since only in case of test level it will
# try to capture stacktrace in case of big untracked allocation)
$CLICKHOUSE_LOCAL --log-level=test 'select throwIf(1) -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }' >& /dev/null
