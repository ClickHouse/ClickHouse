#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

clickhouse-client -q "SHOW PROCESSLIST" &>/dev/null
clickhouse-client -q "SHOW DATABASES" &>/dev/null
clickhouse-client -q "SHOW TABLES" &>/dev/null
