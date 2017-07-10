#!/usr/bin/env bash

clickhouse-client --query="select toUInt64(pow(2, 62)) as value  format JSON" --output_format_json_quote_64bit_integers=0 | grep value
clickhouse-client --query="select toUInt64(pow(2, 62)) as value format JSON" --output_format_json_quote_64bit_integers=1 | grep value

clickhouse-client --user readonly --multiquery --query="set output_format_json_quote_64bit_integers=1 ; select toUInt64(pow(2, 63)) as value format JSON" 2>&1 | grep -o 'value\|Cannot execute SET query in readonly mode.'
clickhouse-client --user readonly --multiquery --query="set output_format_json_quote_64bit_integers=0 ; select toUInt64(pow(2, 63)) as value format JSON" 2>&1 | grep -o 'value\|Cannot execute SET query in readonly mode.'

curl -sS 'http://localhost:8123/?query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=1' | grep value
curl -sS 'http://localhost:8123/?query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=0' | grep value

curl -sS 'http://localhost:8123/?user=readonly&query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=1' 2>&1 | grep -o 'value\|Cannot override setting'
curl -sS 'http://localhost:8123/?user=readonly&query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=0' 2>&1 | grep -o 'value\|Cannot override setting'
