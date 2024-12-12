#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function perform()
{
    local test_id=$1
    local query=$2
    local sort=${3:-1}

    echo "performing test: $test_id"
    ${CLICKHOUSE_CLIENT} --query "$query"
    code=$?

    if [ "$code" -eq 0 ]; then
        for f in "${CLICKHOUSE_TMP}"/*
        do
            echo "file: $(basename "$f")"
            if [ "$sort" -eq 1 ]; then
                sort < "$f"
            else
                cat "$f"
            fi
        done
    else
        echo "query failed"
    fi
    rm -f "${CLICKHOUSE_TMP}"/*
}

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS \`${CLICKHOUSE_TEST_NAME}\`;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE \`${CLICKHOUSE_TEST_NAME}\` (a String, b UInt64, c UInt64) Engine=Memory;"
${CLICKHOUSE_CLIENT} --query "INSERT INTO \`${CLICKHOUSE_TEST_NAME}\` VALUES ('x', 1, 1), ('x', 2, 2), ('x', 3, 3), ('y', 1, 4), ('y', 2, 5), ('y', 3, 6);"

perform "simple__identifier" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP}/{a}' PARTITION BY a;"
perform "simple__partition_id" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP}/{_partition_id}' PARTITION BY a;"

perform "simple__expr_with_alias" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP}/{mod}' PARTITION BY b % 2 as mod;"
perform "simple__expr_without_alias" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP}/{modulo(b, 2)}' PARTITION BY modulo(b, 2);"
perform "simple__const" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP}/{42}' PARTITION BY 42;"

perform "simple__tulpe" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP}/{b}_{c}' PARTITION BY (b, c);"

perform "simple__all_keys" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP}/{c}_{b}_{a}' PARTITION BY a, b, c;"
perform "simple__can_reuse" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP}/{a}_{a}' PARTITION BY a;"

perform "escape__outside" "SELECT 42 INTO OUTFILE '${CLICKHOUSE_TMP}/outside_\{ {42} \}' PARTITION BY 42"
perform "escape__inside" "SELECT 42 as \` {42} \` INTO OUTFILE '${CLICKHOUSE_TMP}/inside_{ \{42\} }' PARTITION BY \` {42} \`"
perform "escape__heredoc" "SELECT 42 INTO OUTFILE \$heredoc\$${CLICKHOUSE_TMP}/heredoc_\{{42}\}\$heredoc\$ PARTITION BY 42"

touch "${CLICKHOUSE_TMP}/{42}"
perform "no_precheck_for_tempalte_itself" "SELECT 42 INTO OUTFILE '${CLICKHOUSE_TMP}/{42}' PARTITION BY 42;"

echo 42 > "${CLICKHOUSE_TMP}/42"
perform "simple_append" "SELECT 42 INTO OUTFILE '${CLICKHOUSE_TMP}/{42}' APPEND PARTITION BY 42;"

echo 42 > "${CLICKHOUSE_TMP}/42"
perform "simple_truncate" "SELECT 42 INTO OUTFILE '${CLICKHOUSE_TMP}/{42}' TRUNCATE PARTITION BY 42;"

perform "simple_format_with_prefix_and_suffix" "SELECT 42 INTO OUTFILE '${CLICKHOUSE_TMP}/{42}'  PARTITION BY 42 FORMAT JSONEachRow SETTINGS output_format_json_array_of_rows = 1" 0

echo "perform file exist test"

touch "${CLICKHOUSE_TMP}/y_2"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP}/{a}_{b}' PARTITION BY a, b;" 2>&1 | grep -Fc "File exists"
rm "${CLICKHOUSE_TMP}"/*

echo "perform wrong template test"

${CLICKHOUSE_CLIENT} --query "SELECT 1 as a INTO OUTFILE '${CLICKHOUSE_TMP}/outfile' PARTITION BY a;" 2>&1 | grep -Fc "Missed columns"

${CLICKHOUSE_CLIENT} --query "SELECT 1 as a INTO OUTFILE '${CLICKHOUSE_TMP}/outfile_{}' PARTITION BY a;" 2>&1 | grep -Fc "Unexpected column name"

${CLICKHOUSE_CLIENT} --query "SELECT 1 as a INTO OUTFILE '${CLICKHOUSE_TMP}/outfile_{b}' PARTITION BY a;" 2>&1 | grep -Fc "Unexpected column name"

${CLICKHOUSE_CLIENT} --query "SELECT 1 as a, 2 as b INTO OUTFILE '${CLICKHOUSE_TMP}/outfile_{_partition_id}_{b}' PARTITION BY a, b;" 2>&1 | grep -Fc "Can only use {_partition_id} with one key"

