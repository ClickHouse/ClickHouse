#!/bin/sh

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

clickhouse-client --host=localhost --query="SELECT 1";
clickhouse-client --host localhost --query "SELECT 1";
clickhouse-client -hlocalhost -q"SELECT 1";
