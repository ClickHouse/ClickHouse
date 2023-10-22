#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_URL="$CLICKHOUSE_URL&http_write_exception_in_output_format=1"

echo "INSERT WITH default_format=JSON"
echo "INSERT INTO system.numbers Select * from numbers(10);" \
  | ${CLICKHOUSE_CURL} -sS "${CH_URL}&default_format=JSON" -i | grep 'xception\|Content-Type' | sed 's/Exception/Ex---tion/' | awk '{ print $1 $2 $3 }'
echo ""
echo "INSERT WITH default_format=XML"
echo "INSERT INTO system.numbers Select * from numbers(10);" \
  | ${CLICKHOUSE_CURL} -sS "${CH_URL}&default_format=XML" -i | grep 'xception\|Content-Type' | sed 's/Exception/Ex---tion/' | awk '{ print $1 $2 $3 }'
echo ""
echo "INSERT WITH default_format=BADFORMAT"
echo "INSERT INTO system.numbers Select * from numbers(10);" \
  | ${CLICKHOUSE_CURL} -sS "${CH_URL}&default_format=BADFORMAT" -i | grep 'xception\|Content-Type' | sed 's/Exception/Ex---tion/' | awk '{ print $1 $2 $3 }'


echo ""
echo "INSERT WITH X-ClickHouse-Format: JSON"
echo "INSERT INTO system.numbers Select * from numbers(10);" \
  | ${CLICKHOUSE_CURL} -sS "${CH_URL}" -H 'X-ClickHouse-Format: JSON' -i | grep 'xception\|Content-Type' | sed 's/Exception/Ex---tion/' | awk '{ print $1 $2 $3 }'
echo ""
echo "INSERT WITH X-ClickHouse-Format: XML"
echo "INSERT INTO system.numbers Select * from numbers(10);" \
  | ${CLICKHOUSE_CURL} -sS "${CH_URL}" -H 'X-ClickHouse-Format: XML' -i | grep 'xception\|Content-Type' | sed 's/Exception/Ex---tion/' | awk '{ print $1 $2 $3 }'
echo ""
echo "INSERT WITH X-ClickHouse-Format: BADFORMAT"
echo "INSERT INTO system.numbers Select * from numbers(10);" \
  | ${CLICKHOUSE_CURL} -sS "${CH_URL}" -H 'X-ClickHouse-Format: BADFORMAT' -i | grep 'xception\|Content-Type' | sed 's/Exception/Ex---tion/' | awk '{ print $1 $2 $3 }'
