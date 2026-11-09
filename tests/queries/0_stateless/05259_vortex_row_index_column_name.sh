#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The row numbers of a Vortex scan come through an extra column the scan prepends to its output.
# It is an internal helper, so a file that has a column of the same name must still be readable
# with `_row_number` and with lazy materialization, which both ask for the row numbers.

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05259_vortex_row_index_column_name_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

DATA_DIR="${LOCAL_DIR}/user_files"
mkdir -p "$DATA_DIR"

"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/data.vortex', Vortex)
    SELECT number * 10 AS _row_index, concat('val_', toString(number)) AS __row_index FROM numbers(10)
    SETTINGS engine_file_truncate_on_insert = 1;
"

"${LOCAL[@]}" --enable_analyzer=1 --query "
    SELECT '-- the columns of the file next to the row number';
    SELECT _row_number, _row_index, __row_index FROM file('${DATA_DIR}/data.vortex', Vortex) ORDER BY _row_number LIMIT 3;

    SELECT '-- a lazily materialized read';
    SELECT _row_index, __row_index
    FROM file('${DATA_DIR}/data.vortex', Vortex)
    ORDER BY _row_index DESC LIMIT 3
    SETTINGS query_plan_optimize_lazy_materialization = 1,
             query_plan_optimize_lazy_materialization_for_file = 1,
             query_plan_max_limit_for_lazy_materialization = 0;
"
