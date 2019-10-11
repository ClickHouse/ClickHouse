#!/usr/bin/env bash

# We should have correct env vars from shell_config.sh to run this test
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

export CH_C_SETTINGS='--format_custom_field_delimiter=\  --format_custom_row_before_delimiter= --format_custom_row_between_delimiter= --format_custom_result_before_delimiter= --format_custom_result_after_delimiter='
# in order to check queries individually (does not stop on the first one that fails):
IFS=$'\n'; for I in $($CURDIR/00921_datetime64_compatibility.python) ; do "${CLICKHOUSE_CLIENT}" -nm -q "$I" $CH_C_SETTINGS; echo ; done 2>&1 \
    | tr "\n" "\t" \
    | sed -Eu 's/-------+\t+/\n/g; s/Received exception from server \(version 19.16.1\):\s+//g; s/DB::Exception: Received from localhost:9000. //g; s/\s+$//g; '
