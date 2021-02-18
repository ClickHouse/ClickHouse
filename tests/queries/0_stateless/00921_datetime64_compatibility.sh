#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL="none"
# We should have correct env vars from shell_config.sh to run this test
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# In order to check queries individually (don't stop on the first one that fails):
#IFS=$'\n'; for I in $(${CURDIR}/00921_datetime64_compatibility.python) ; do unset IFS; ${CLICKHOUSE_CLIENT} --query "${I}"; echo ; done 2>&1;

# ${CURDIR}/00921_datetime64_compatibility.python

"${CURDIR}"/00921_datetime64_compatibility.python \
    | ${CLICKHOUSE_CLIENT} --ignore-error -T -nm --calculate_text_stack_trace 0 --log-level 'error' 2>&1 \
    | sed -Ee 's/Received exception from server .*//g; s/(Code: [0-9]+). DB::Exception: Received from .* DB::Exception/\1/g'
