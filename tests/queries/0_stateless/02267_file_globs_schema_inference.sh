#!/usr/bin/env bash
# Tags: no-fasttest
# Fast test: Requires JSON

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


PREFIX="02267_PREFIX_${CLICKHOUSE_DATABASE}"

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
trap cleanup EXIT
function cleanup()
{
    rm -f "${USER_FILES_PATH}"/"${PREFIX}"*
}

${CLICKHOUSE_CLIENT} --query="insert into function file('${PREFIX}_data2.jsonl') select NULL as x;"
${CLICKHOUSE_CLIENT} --query="insert into function file('${PREFIX}_data3.jsonl') select * from numbers(0);"
${CLICKHOUSE_CLIENT} --query="insert into function file('${PREFIX}_data4.jsonl') select 1 as x;"

${CLICKHOUSE_CLIENT} --query="select * from file('${PREFIX}_data*.jsonl') order by x;"

${CLICKHOUSE_CLIENT} --query="insert into function file('${PREFIX}_data1.jsonl', 'TSV') select 1 as x;"
${CLICKHOUSE_CLIENT} --query="insert into function file('${PREFIX}_data1.jsonl', 'TSV') select [1,2,3] as x;"

${CLICKHOUSE_CLIENT} --schema_inference_use_cache_for_file=0 --query="select * from file('${PREFIX}_data*.jsonl') order by x" 2>&1 | (grep -q "INCORRECT_DATA" || echo "Expected error not found")
