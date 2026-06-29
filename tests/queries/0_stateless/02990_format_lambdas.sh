#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY="SELECT lambda(1, 1)"; QUERY2=$(${CLICKHOUSE_FORMAT} --query "$QUERY"); echo "$QUERY2"; QUERY3=$(${CLICKHOUSE_FORMAT} --query "$QUERY2"); echo "$QUERY3";
QUERY="SELECT lambda(x, 1)"; QUERY2=$(${CLICKHOUSE_FORMAT} --query "$QUERY"); echo "$QUERY2"; QUERY3=$(${CLICKHOUSE_FORMAT} --query "$QUERY2"); echo "$QUERY3";
QUERY="SELECT lambda((x, y), 1)"; QUERY2=$(${CLICKHOUSE_FORMAT} --query "$QUERY"); echo "$QUERY2"; QUERY3=$(${CLICKHOUSE_FORMAT} --query "$QUERY2"); echo "$QUERY3";
QUERY="SELECT lambda(f(1), 1)"; QUERY2=$(${CLICKHOUSE_FORMAT} --query "$QUERY"); echo "$QUERY2"; QUERY3=$(${CLICKHOUSE_FORMAT} --query "$QUERY2"); echo "$QUERY3";
QUERY="SELECT lambda(f(x), 1)"; QUERY2=$(${CLICKHOUSE_FORMAT} --query "$QUERY"); echo "$QUERY2"; QUERY3=$(${CLICKHOUSE_FORMAT} --query "$QUERY2"); echo "$QUERY3";
