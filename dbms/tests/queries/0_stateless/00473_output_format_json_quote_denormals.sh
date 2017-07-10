#!/usr/bin/env bash

clickhouse-client --query="select 1/0, -1/0, sqrt(-1), -sqrt(-1) format JSON" --output_format_json_quote_denormals=0 | grep -o null
clickhouse-client --query="select 1/0, -1/0, sqrt(-1), -sqrt(-1) format JSONCompact" --output_format_json_quote_denormals=0 | grep -o null
clickhouse-client --query="select 1/0, -1/0, sqrt(-1), -sqrt(-1) format JSONEachRow" --output_format_json_quote_denormals=0 | grep -o null

clickhouse-client --query="select 1/0, -1/0, sqrt(-1), -sqrt(-1) format JSON" --output_format_json_quote_denormals=1 | grep -o "inf\|-inf\|nan"
clickhouse-client --query="select 1/0, -1/0, sqrt(-1), -sqrt(-1) format JSONCompact" --output_format_json_quote_denormals=1 | grep -o "inf\|-inf\|nan"
clickhouse-client --query="select 1/0, -1/0, sqrt(-1), -sqrt(-1) format JSONEachRow" --output_format_json_quote_denormals=1 | grep -o "inf\|-inf\|nan"
