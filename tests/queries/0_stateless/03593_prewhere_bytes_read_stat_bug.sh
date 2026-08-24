#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "
create table t (x Int8, s String) engine MergeTree order by x;
insert into t select number, randomString(1000) from numbers(10);"

json=`$CLICKHOUSE_CLIENT -q "select sum(cityHash64(s)) from t prewhere x < 100 format Json"`
bytes_read=`echo "$json" | grep -Po '(?<="bytes_read": )\d+'`
if [ "$bytes_read" -gt 5000 ]
then
    echo "oki doki"
else
    echo "bytes_read too small: $bytes_read"
    exit 1
fi
