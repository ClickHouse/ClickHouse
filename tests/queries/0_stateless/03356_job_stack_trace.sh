#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

if [[ $(${CLICKHOUSE_LOCAL} --stacktrace --query "SELECT throwIf(number) FROM numbers_mt(100000000) SETTINGS enable_job_stack_trace=0" 2>&1 | grep -c "Job's origin stack trace:") -ne 0 ]]; then
    echo FAIL with enable_job_stack_trace=0
fi

if [[ $(${CLICKHOUSE_LOCAL} --stacktrace --query "SELECT throwIf(number) FROM numbers_mt(100000000) SETTINGS enable_job_stack_trace=1" 2>&1 | grep -c "Job's origin stack trace:") -eq 0 ]]; then
    echo FAIL with enable_job_stack_trace=1
fi
