#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

cp /dev/null 00965_logs_level_bugfix.tmp

clickhouse-client --send_logs_level="debug" --query="SELECT 1;" 2>> 00965_logs_level_bugfix.tmp
awk '{ print $8 }' 00965_logs_level_bugfix.tmp

 
