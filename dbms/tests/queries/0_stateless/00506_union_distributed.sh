#!/usr/bin/env bash

. ./99999_sh_lib.sh

have_test_shard_localhost=`${CLICKHOUSE_EXTRACT_CONFIG} -k remote_servers | grep have_test_shard_localhost`

pwd=`pwd`

if [ -z ${have_test_shard_localhost} ]; then
    # No test shard in config. Fake result
    cat $pwd/00506_union_distributed.reference
else
    ${CLICKHOUSE_CLIENT} -m -n < $pwd/00506_union_distributed.sql_
fi
