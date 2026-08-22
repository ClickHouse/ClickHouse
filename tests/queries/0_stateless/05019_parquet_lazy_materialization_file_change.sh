#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files

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

    INSERT INTO FUNCTION file('${LOCAL_DIR}/replacement.parquet', Parquet)
    SELECT number AS k, concat('new_', toString(number)) AS s
    FROM numbers(1000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
"

# The inode of the generation the main pass must see. The replacement is installed with mv,
# so seeing a query fd with this inode proves the main pass opened the file before the swap.
orig_inode=$(stat -c %i "${LOCAL_DIR}/data.parquet")

# `sleepEachRow` keeps the main pass open while a replacement is atomically installed. The
# lazy pass must reject the replacement instead of combining columns from the two generations.
# A single reading thread makes the sleeping serial, so the main pass stays open for the whole
# two seconds rather than for the time of one row group.
# The structure is spelled out because schema inference would open the file too: waiting for
# "any fd on data.parquet" could then trigger on that short-lived inference read, letting the
# swap land before the main pass opens the file at all - the query then just reads the
# replacement as the only generation it ever saw, a legitimate result that produces no error.
"${LOCAL[@]}" \
    --enable_analyzer=1 \
    --max_block_size=1 \
    --max_threads=1 \
    --max_parsing_threads=1 \
    --query_plan_optimize_lazy_materialization=1 \
    --query_plan_max_limit_for_lazy_materialization=0 \
    --query_plan_optimize_lazy_materialization_for_file=1 \
    --query "SELECT s FROM file('${LOCAL_DIR}/data.parquet', Parquet, 'k UInt64, s String') PREWHERE sleepEachRow(0.002) = 0 ORDER BY k LIMIT 1" \
    > "${LOCAL_DIR}/query.out" 2>&1 &
query_pid=$!

# Wait until the main pass really holds the original generation open: an fd whose target is
# the pre-swap inode. Waiting a fixed delay is not enough (process startup alone can take
# longer under a sanitizer build), and merely seeing a path match is not enough either - only
# the inode proves the fd is not already a reopen of some later generation.
for _ in {1..1200}; do
    for fd in "/proc/${query_pid}/fd"/*; do
        if [ "$(stat -Lc %i "${fd}" 2>/dev/null)" = "${orig_inode}" ]; then
            break 2
        fi
    done
    kill -0 "${query_pid}" 2>/dev/null || break
    sleep 0.05
done

mv "${LOCAL_DIR}/replacement.parquet" "${LOCAL_DIR}/data.parquet"
wait "${query_pid}" || true

if grep -q 'FILE_CHANGED_DURING_READ' "${LOCAL_DIR}/query.out"; then
    echo 'FILE_CHANGED_DURING_READ'
else
    cat "${LOCAL_DIR}/query.out" >&2
    exit 1
fi
