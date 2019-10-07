#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

> 00965_send_logs_level_concurrent_queries_first.tmp
> 00965_send_logs_level_concurrent_queries_second.tmp

clickhouse-client --send_logs_level="trace" --query="SELECT * from numbers(100000);" >> /dev/null 2>> 00965_send_logs_level_concurrent_queries_first.tmp &
clickhouse-client --send_logs_level="information" --query="SELECT * from numbers(100000);" >> /dev/null 2>> 00965_send_logs_level_concurrent_queries_second.tmp

sleep 2

awk '{ print $8 }' 00965_send_logs_level_concurrent_queries_first.tmp
echo "*****"
awk '{ print $8 }' 00965_send_logs_level_concurrent_queries_second.tmp

