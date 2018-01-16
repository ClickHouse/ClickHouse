#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

TZ=Europe/Moscow ${CLICKHOUSE_LOCAL} -s --query="SELECT toDateTime('1990-10-19 00:00:00')"
TZ=Asia/Colombo ${CLICKHOUSE_LOCAL} -s --query="SELECT toDateTime('1990-10-19 00:00:00')"
TZ=Asia/Kathmandu ${CLICKHOUSE_LOCAL} -s --query="SELECT toDateTime('1990-10-19 00:00:00')"
