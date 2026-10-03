#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files
# Fail point state is process global and each clickhouse-local invocation owns its own, so arming
# one here cannot reach a concurrent test: no no-parallel tag is needed.

# A file removed between the single-file parallel split decision and the per-bucket reads is a
# concurrent modification, so the per-bucket sources fail close with `FILE_CHANGED_DURING_READ`
# even under `engine_file_empty_if_not_exists = 1`: other sources may have already opened the old
# file and keep reading it, and treating the file as empty in one source would silently return
# only part of the row groups. The setting still applies to a file that is missing when the read
# is planned, which is never split.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05230_split_missing_file_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

# 300000 rows at row-group size 2000 => 150 row groups, uncompressed, so the split floors are
# cleared and the read fans out into several per-bucket sources.
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${LOCAL_DIR}/data.parquet', Parquet)
    SELECT number AS k, repeat('a', 200) AS s FROM numbers(300000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 2000,
        output_format_parquet_compression_method = 'none';
"

split=(--max_threads=8
       --input_format_parquet_min_bytes_to_split=1
       --input_format_parquet_bytes_per_split_bucket=1024)
query="SELECT sum(k) FROM file('${LOCAL_DIR}/data.parquet', Parquet, 'k UInt64, s String')"

# The read is split (`File × N` in the pipeline) - otherwise the per-bucket path below is not the
# one being exercised.
"${LOCAL[@]}" "${split[@]}" --query "EXPLAIN PIPELINE ${query}" | grep -c -E '^ *File × [0-9]+'

# Untouched file: the split reads every row group exactly once.
"${LOCAL[@]}" "${split[@]}" --query "${query}"

# The file is gone by the time the per-bucket sources open it. Even with the setting on, that is
# an error, not an empty (or partial) result. Only the code is printed: the message carries a path
# that differs every run.
"${LOCAL[@]}" "${split[@]}" --engine_file_empty_if_not_exists=1 --query "
    SYSTEM ENABLE FAILPOINT file_read_inject_fixed_file_missing;
    ${query};
" 2>&1 | grep -o -m1 'FILE_CHANGED_DURING_READ'

# The same with the setting off.
"${LOCAL[@]}" "${split[@]}" --engine_file_empty_if_not_exists=0 --query "
    SYSTEM ENABLE FAILPOINT file_read_inject_fixed_file_missing;
    ${query};
" 2>&1 | grep -o -m1 'FILE_CHANGED_DURING_READ'

# A file that is missing when the read is planned is not split, and the setting makes it empty.
"${LOCAL[@]}" "${split[@]}" --engine_file_empty_if_not_exists=1 --query "
    SELECT count() FROM file('${LOCAL_DIR}/missing.parquet', Parquet, 'k UInt64, s String');
"
