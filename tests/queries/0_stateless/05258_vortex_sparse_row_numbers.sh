#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ChunkInfoRowNumbers` stores the row numbers of a chunk as an offset and a filter that spans every
# row between the first and the last one, so the Vortex reader puts rows that are far apart in
# different chunks instead of allocating a filter in proportion to the gap. The row numbers must
# stay exact across such a cut. The rows kept here are two million rows apart, well past the 1 MiB
# a filter may span.

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05258_vortex_sparse_row_numbers_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

DATA_DIR="${LOCAL_DIR}/user_files"
mkdir -p "$DATA_DIR"

"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/data.vortex', Vortex)
    SELECT number AS k FROM numbers(5000000)
    SETTINGS engine_file_truncate_on_insert = 1;
"

"${LOCAL[@]}" --enable_analyzer=1 --query "
    SELECT '-- the second pass reads no column of the file, only the selected rows';
    SELECT _row_number, absent
    FROM file('${DATA_DIR}/data.vortex', Vortex, 'absent UInt64')
    ORDER BY _row_number % 2000000, _row_number LIMIT 6
    SETTINGS query_plan_optimize_lazy_materialization = 1,
             query_plan_optimize_lazy_materialization_for_file = 1,
             query_plan_max_limit_for_lazy_materialization = 0;

    SELECT '-- the second pass reads a column of the file at the selected rows';
    SELECT _row_number, k
    FROM file('${DATA_DIR}/data.vortex', Vortex)
    ORDER BY k % 2000000, k LIMIT 6
    SETTINGS query_plan_optimize_lazy_materialization = 1,
             query_plan_optimize_lazy_materialization_for_file = 1,
             query_plan_max_limit_for_lazy_materialization = 0;

    SELECT '-- a selective filter keeps rows that are far apart';
    SELECT _row_number, k
    FROM file('${DATA_DIR}/data.vortex', Vortex)
    WHERE k IN (3, 4, 2500000, 4999999)
    ORDER BY _row_number;
"
