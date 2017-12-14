#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

clickhouse-client --query="select toUInt64(pow(2, 62)) as value format JSON" --output_format_json_quote_64bit_integers=0 | grep value
clickhouse-client --query="select toUInt64(pow(2, 62)) as value format JSON" --output_format_json_quote_64bit_integers=1 | grep value

clickhouse-client --readonly=1 --multiquery --query="set output_format_json_quote_64bit_integers=1 ; select toUInt64(pow(2, 63)) as value format JSON" 2>&1 | grep -o 'value\|Cannot execute SET query in readonly mode.'
clickhouse-client --readonly=1 --multiquery --query="set output_format_json_quote_64bit_integers=0 ; select toUInt64(pow(2, 63)) as value format JSON" 2>&1 | grep -o 'value\|Cannot execute SET query in readonly mode.'

curl -sS 'http://localhost:8123/?query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=1' | grep value
curl -sS 'http://localhost:8123/?query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=0' | grep value

curl -sS 'http://localhost:8123/?session_id=readonly' -d 'SET readonly = 1'

curl -sS 'http://localhost:8123/?session_id=readonly&query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=1' 2>&1 | grep -o 'value\|Cannot override setting'
curl -sS 'http://localhost:8123/?session_id=readonly&query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=0' 2>&1 | grep -o 'value\|Cannot override setting'
