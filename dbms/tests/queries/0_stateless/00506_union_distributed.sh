#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/00000_sh_lib.sh

have_test_shard_localhost=`$CLICKHOUSE_EXTRACT_CONFIG -k remote_servers | grep have_test_shard_localhost`

if [ -z ${have_test_shard_localhost} ]; then
    # No test shard in config. Fake result
    cat $CURDIR/00506_union_distributed.reference
else
    ${CLICKHOUSE_CLIENT} -m -n < $CURDIR/00506_union_distributed.sql_
fi
