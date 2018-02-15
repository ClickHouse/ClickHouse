#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

function perform()
{
    local test_id=$1
    local query=$2

    echo "performing test: $test_id"
    ${CLICKHOUSE_CLIENT} --query "$query" 2>/dev/null
    if [ "$?" -eq 0 ]; then
        cat "${CLICKHOUSE_TMP}/test_into_outfile_$test_id.out"
    else
        echo "query failed"
    fi
    rm -f "${CLICKHOUSE_TMP}/test_into_outfile_$test_id.out"
}

perform "select" "SELECT 1, 2, 3 INTO OUTFILE '${CLICKHOUSE_TMP}/test_into_outfile_select.out'"

perform "union_all" "SELECT 1, 2 UNION ALL SELECT 3, 4 INTO OUTFILE '${CLICKHOUSE_TMP}/test_into_outfile_union_all.out' FORMAT TSV" | sort --numeric-sort

perform "bad_union_all" "SELECT 1, 2 INTO OUTFILE '${CLICKHOUSE_TMP}/test_into_outfile_bad_union_all.out' UNION ALL SELECT 3, 4"

perform "describe_table" "DESCRIBE TABLE system.one INTO OUTFILE '${CLICKHOUSE_TMP}/test_into_outfile_describe_table.out'"

echo "performing test: clickhouse-local"
echo -e '1\t2' | ${CLICKHOUSE_LOCAL} -s --structure 'col1 UInt32, col2 UInt32' --query "SELECT col1 + 1, col2 + 1 FROM table INTO OUTFILE '${CLICKHOUSE_TMP}/test_into_outfile_clickhouse-local.out'"
if [ "$?" -eq 0 ]; then
    cat "${CLICKHOUSE_TMP}/test_into_outfile_clickhouse-local.out"
else
    echo "query failed"
fi
rm -f "${CLICKHOUSE_TMP}/test_into_outfile_clickhouse-local.out"

echo "performing test: http"
echo "SELECT 1, 2 INTO OUTFILE '${CLICKHOUSE_TMP}/test_into_outfile_http.out'" | ${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}" -d @- --fail || echo "query failed"
