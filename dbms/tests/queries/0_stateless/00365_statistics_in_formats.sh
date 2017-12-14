#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

clickhouse-client --query="SELECT number FROM system.numbers LIMIT 10 FORMAT JSON" | grep 'rows_read';
clickhouse-client --query="SELECT number FROM system.numbers LIMIT 10 FORMAT JSONCompact" | grep 'rows_read';
clickhouse-client --query="SELECT number FROM system.numbers LIMIT 10 FORMAT XML" | grep 'rows_read';

curl -sS 'http://localhost:8123/' -d "SELECT number FROM system.numbers LIMIT 10 FORMAT JSON" | grep 'rows_read';
curl -sS 'http://localhost:8123/' -d "SELECT number FROM system.numbers LIMIT 10 FORMAT JSONCompact" | grep 'rows_read';
curl -sS 'http://localhost:8123/' -d "SELECT number FROM system.numbers LIMIT 10 FORMAT XML" | grep 'rows_read';
