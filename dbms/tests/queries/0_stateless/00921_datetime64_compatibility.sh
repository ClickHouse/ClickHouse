#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test

python $CURDIR/00921_datetime64_compatibility.python | ${CLICKHOUSE_CLIENT} -nm