#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_dicts"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_dicts (a LowCardinality(String)) engine=MergeTree order by a"
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES arrow_dicts"
${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_dicts select toString(number) from numbers(120);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_dicts select toString(number) from numbers(120, 120);"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_dicts FORMAT Arrow SETTINGS output_format_arrow_low_cardinality_as_dictionary=1" > "${CLICKHOUSE_TMP}"/$CLICKHOUSE_TEST_UNIQUE_NAME.arrow

${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_dicts"

$CLICKHOUSE_LOCAL -q "select uniqExact(a) from file('$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME.arrow')"

$CLICKHOUSE_LOCAL -q "select * from file('$CUR_DIR/data_arrow/different_dicts.arrowstream') order by x"

