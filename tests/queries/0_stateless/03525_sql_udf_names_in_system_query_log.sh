#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

check_sql_udf_functions() {
    if [ "${#}" == "0" ]; then
        return
    fi

    # In order for the test to run in parallel,
    # we add the name of the database to the function name, because it has a random value.
    funcs=""
    for func in ${@}; do
        funcs+="${func}_${CLICKHOUSE_DATABASE} "
    done

    for func in ${funcs}; do
        $CLICKHOUSE_CLIENT -q "
            DROP FUNCTION IF EXISTS ${func};
            CREATE FUNCTION ${func} AS (input) -> input"
    done

    execute_sql_udf=""
    if [ "${#}" == "1" ]; then
        execute_sql_udf="${funcs}(8)"
    else
        joined_string=""
        for func in ${funcs}; do
            if [ -n "${joined_string}" ]; then
                joined_string+=", "
            fi
            joined_string+="${func}(8)"
        done
        execute_sql_udf="concat(${joined_string})"
    fi

    query_id=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")
    $CLICKHOUSE_CLIENT --query_id=${query_id} -q "SELECT ${execute_sql_udf}"

    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS query_log;
        SELECT arraySort(used_sql_user_defined_functions) FROM system.query_log
        WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND query_id = '${query_id}'"

    for func in ${funcs}; do
        $CLICKHOUSE_CLIENT -q "DROP FUNCTION IF EXISTS ${func}"
    done
}

test_sql_udf_single() {
    check_sql_udf_functions ${FUNCNAME}
}

test_sql_udf_multiple() {
    check_sql_udf_functions "${FUNCNAME}_1" "${FUNCNAME}_2" "${FUNCNAME}_3"
}

test_sql_udf_duplicate() {
    check_sql_udf_functions ${FUNCNAME} ${FUNCNAME} ${FUNCNAME}
}

test_sql_udf_single
test_sql_udf_multiple
test_sql_udf_duplicate
