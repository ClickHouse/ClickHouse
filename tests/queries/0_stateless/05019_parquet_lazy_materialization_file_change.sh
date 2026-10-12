#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files
# Fail point state is process global and each clickhouse-local invocation owns its own, so arming
# one here cannot reach a concurrent test: no no-parallel tag is needed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05019_lazy_mat_file_change_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${LOCAL_DIR}/data.parquet', Parquet)
    SELECT number AS k, concat('old_', toString(number)) AS s
    FROM numbers(1000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
"

lazy=(--enable_analyzer=1
      --query_plan_optimize_lazy_materialization=1
      --query_plan_max_limit_for_lazy_materialization=0
      --query_plan_optimize_lazy_materialization_for_file=1)
query="SELECT s FROM file('${LOCAL_DIR}/data.parquet', Parquet, 'k UInt64, s String') ORDER BY k LIMIT 1"

# The lazy pass rereads the file by path to fetch the deferred column for the row numbers the main
# pass picked, so an untouched file must read through both passes.
"${LOCAL[@]}" "${lazy[@]}" --query "${query}"

# When that reread lands on a different generation of the file, the deferred column must not be
# combined with row numbers picked from the previous one. Only the code is printed: the message
# carries a path that differs every run.
"${LOCAL[@]}" "${lazy[@]}" --query "
    SYSTEM ENABLE FAILPOINT file_read_inject_version_token_mismatch;
    ${query}
" 2>&1 | grep -o -m1 'FILE_CHANGED_DURING_READ'
