#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

curl -sS 'http://localhost:8123/' -d 'SELECT a' | wc -l
curl -sS 'http://localhost:8123/?stacktrace=0' -d 'SELECT a' | wc -l
[[ $(curl -sS 'http://localhost:8123/?stacktrace=1' -d 'SELECT a' | wc -l) -gt 3 ]] && echo 'Ok' || echo 'Fail'
curl -sS 'http://localhost:8123/' -d "SELECT intDiv(number, 0) FROM remote('127.0.0.{1,2}', system.numbers)" | wc -l

$CLICKHOUSE_CLIENT --query="SELECT a" 2>&1 | wc -l
[[ $($CLICKHOUSE_CLIENT --query="SELECT a" --stacktrace 2>&1 | wc -l) -gt 3 ]] && echo 'Ok' || echo 'Fail'
$CLICKHOUSE_CLIENT --query="SELECT intDiv(number, 0) FROM remote('127.0.0.{1,2}', system.numbers)" 2>&1 | wc -l
