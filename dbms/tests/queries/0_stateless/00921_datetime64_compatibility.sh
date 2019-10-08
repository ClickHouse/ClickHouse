#!/usr/bin/env bash

# We should have correct env vars from shell_config.sh to run this test
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# in order to check queries individually (does not stop on the first one that fails):
IFS=$'\n'; for I in $($CURDIR/00921_datetime64_compatibility.python) ; do "${CLICKHOUSE_CLIENT}" -nm -q "$I"; echo ; done # 2>&1 | tee datetime64_compat.log
