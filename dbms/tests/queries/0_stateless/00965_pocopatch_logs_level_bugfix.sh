#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

> 00965_logs_level_bugfix.tmp

clickhouse-client --send_logs_level="trace" --query="SELECT 1;" 2>> 00965_logs_level_bugfix.tmp
clickhouse-client --send_logs_level="debug" --query="SELECT 1;" 2>> 00965_logs_level_bugfix.tmp
clickhouse-client --send_logs_level="information" --query="SELECT 1;" 2>> 00965_logs_level_bugfix.tmp
clickhouse-client --send_logs_level="warning" --query="SELECT 1;" 2>> 00965_logs_level_bugfix.tmp
clickhouse-client --send_logs_level="error" --query="SELECT 1;" 2>> 00965_logs_level_bugfix.tmp
clickhouse-client --send_logs_level="none" --query="SELECT 1;" 2>> 00965_logs_level_bugfix.tmp

awk '{ print $8 }' 00965_logs_level_bugfix.tmp


