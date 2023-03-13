#!/usr/bin/env bash
# Tags: no-ubsan, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/data_extract_key_value_pairs/big_input_file.txt
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS json_map"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE json_map (map Map(String, String)) ENGINE = Memory"
cat "$DATA_FILE" | ${CLICKHOUSE_CLIENT} -q "INSERT INTO json_map SELECT extractKeyValuePairs(line, '\\\', ':', ',', '\"') FROM input('line String') FORMAT LineAsString"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM json_map" | md5sum
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM json_map"
${CLICKHOUSE_CLIENT} --query="drop table json_map"
