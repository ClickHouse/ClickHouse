#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

curl -sS -F param_id=1 -X POST "${CLICKHOUSE_URL}&query=select%201%20as%20c%20where%20c%20%3D%20%7Bid%3AUInt8%7D";
curl -sS -X GET "${CLICKHOUSE_URL}&query=select%201%20as%20c%20where%20c%20%3D%20%7Bid%3AUInt8%7D&param_id=1";
curl -sS -F param_id=1 -X POST "${CLICKHOUSE_URL}&query=select%201%20as%20c%2C%202%20as%20c2%20where%20c%20%3D%20%7Bid%3AUInt8%7D%20and%20c2%20%3D%20%20%7Bid2%3AUInt8%7D&param_id2=2";
