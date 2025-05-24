#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# empty directory inside CLICKHOUSE_TMP
CLICKHOUSE_TMP_OUTPUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_NAME}"
mkdir "${CLICKHOUSE_TMP_OUTPUT}"

function perform()
{
    local test_id=$1
    local query=$2
    local sort=${3:-1}

    echo "performing test: $test_id"
    ${CLICKHOUSE_CLIENT} --query "$query"
    code=$?

    if [ "$code" -eq 0 ]; then
        (
            # define order for `{` char
            export LC_ALL=C
            for f in "${CLICKHOUSE_TMP_OUTPUT}"/*
            do
                echo "file: $(basename "$f")"
                if [ "$sort" -eq 1 ]; then
                    sort < "$f"
                else
                    cat "$f"
                fi
            done
        )
    else
        echo "query failed"
    fi
    rm -f "${CLICKHOUSE_TMP_OUTPUT}"/*
}

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS \`${CLICKHOUSE_TEST_NAME}\`;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE \`${CLICKHOUSE_TEST_NAME}\` (a String, b UInt64, c UInt64) Engine=Memory;"
${CLICKHOUSE_CLIENT} --query "INSERT INTO \`${CLICKHOUSE_TEST_NAME}\` VALUES ('x', 1, 1), ('x', 2, 2), ('x', 3, 3), ('y', 1, 4), ('y', 2, 5), ('y', 3, 6);"

perform "simple__identifier" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/{a}' PARTITION BY a;"
perform "simple__partition_id" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/{_partition_id}' PARTITION BY a;"

perform "simple__expr_with_alias" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/{mod}' PARTITION BY b % 2 as mod;"
perform "simple__expr_without_alias" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/{modulo(b, 2)}' PARTITION BY modulo(b, 2);"
perform "simple__const" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/{42}' PARTITION BY 42;"

perform "simple__tulpe" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/{b}_{c}' PARTITION BY (b, c);"

perform "simple__all_keys" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/{c}_{b}_{a}' PARTITION BY a, b, c;"
perform "simple__can_reuse" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/{a}_{a}' PARTITION BY a;"

perform "escape__outside" "SELECT 42 INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/outside_\{ {42} \}' PARTITION BY 42"
perform "escape__inside" "SELECT 42 as \` {42} \` INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/inside_{ \{42\} }' PARTITION BY \` {42} \`"
perform "escape__heredoc" "SELECT 42 INTO OUTFILE \$heredoc\$${CLICKHOUSE_TMP_OUTPUT}/heredoc_\{{42}\}\$heredoc\$ PARTITION BY 42"

touch "${CLICKHOUSE_TMP_OUTPUT}/{42}"
perform "no_precheck_for_tempalte_itself" "SELECT 42 INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/{42}' PARTITION BY 42;"

echo 42 > "${CLICKHOUSE_TMP_OUTPUT}/42"
perform "simple_append" "SELECT 42 INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/{42}' APPEND PARTITION BY 42;"

echo 42 > "${CLICKHOUSE_TMP_OUTPUT}/42"
perform "simple_truncate" "SELECT 42 INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/{42}' TRUNCATE PARTITION BY 42;"

perform "simple_format_with_prefix_and_suffix" "SELECT 42 INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/{42}'  PARTITION BY 42 FORMAT JSONEachRow SETTINGS output_format_json_array_of_rows = 1" 0

echo "perform file exist test"

touch "${CLICKHOUSE_TMP_OUTPUT}/y_2"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/{a}_{b}' PARTITION BY a, b;" 2>&1 | grep -Fc "File exists"
rm "${CLICKHOUSE_TMP_OUTPUT}"/*

echo "perform wrong template test"

${CLICKHOUSE_CLIENT} --query "SELECT 1 as a INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/outfile' PARTITION BY a;" 2>&1 | grep -Fc "Missed columns"

${CLICKHOUSE_CLIENT} --query "SELECT 1 as a INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/outfile_{}' PARTITION BY a;" 2>&1 | grep -Fc "Unexpected column name"

${CLICKHOUSE_CLIENT} --query "SELECT 1 as a INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/outfile_{b}' PARTITION BY a;" 2>&1 | grep -Fc "Unexpected column name"

${CLICKHOUSE_CLIENT} --query "SELECT 1 as a, 2 as b INTO OUTFILE '${CLICKHOUSE_TMP_OUTPUT}/outfile_{_partition_id}_{b}' PARTITION BY a, b;" 2>&1 | grep -Fc "Can only use {_partition_id} with one key"

