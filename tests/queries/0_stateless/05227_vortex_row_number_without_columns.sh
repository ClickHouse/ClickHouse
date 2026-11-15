#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A read that needs no physical column of the file still has to deliver the row numbers a consumer
# asked for: the scan is skipped entirely in that case, and the rows it stands in for must keep
# their `ChunkInfoRowNumbers`, or `_row_number` comes back as `NULL` and lazy materialization has
# nothing to line its two passes up on.

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05227_vortex_row_number_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

DATA_DIR="${LOCAL_DIR}/user_files"
mkdir -p "$DATA_DIR"

"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/data.vortex', Vortex)
    SELECT number AS k, concat('val_', toString(number)) AS s FROM numbers(10)
    SETTINGS engine_file_truncate_on_insert = 1;
"

"${LOCAL[@]}" --enable_analyzer=1 --query "
    SELECT '-- no column of the file is read at all';
    SELECT _row_number FROM file('${DATA_DIR}/data.vortex', Vortex) ORDER BY _row_number;

    SELECT '-- every projected column is absent from the file';
    SELECT _row_number, absent FROM file('${DATA_DIR}/data.vortex', Vortex, 'absent UInt64') ORDER BY _row_number;

    SELECT '-- a column of the file next to the row number, for comparison';
    SELECT _row_number, k FROM file('${DATA_DIR}/data.vortex', Vortex) ORDER BY _row_number LIMIT 3;

    SELECT '-- the row numbers of a lazily materialized read whose eager pass reads no column';
    SELECT _row_number, absent
    FROM file('${DATA_DIR}/data.vortex', Vortex, 'absent UInt64')
    ORDER BY _row_number DESC LIMIT 3
    SETTINGS query_plan_optimize_lazy_materialization = 1,
             query_plan_optimize_lazy_materialization_for_file = 1,
             query_plan_max_limit_for_lazy_materialization = 0;
"
