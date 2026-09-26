#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files
# Fail point state is process global and each clickhouse-local invocation owns its own, so arming
# one here cannot reach a concurrent test: no no-parallel tag is needed.

# `engine_file_empty_if_not_exists` must hold on the single-file parallel split path too. A source
# of the split is assigned one fixed path instead of pulling one from the file iterator, so it does
# not pass the iterator's missing-file check; without its own check, a file removed between the
# split decision and the per-bucket reads turns the setting's empty result into an error.

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

# The file is gone by the time the per-bucket sources open it. With the setting on, that is an
# empty result, exactly as for an unsplit read of a missing file.
"${LOCAL[@]}" "${split[@]}" --engine_file_empty_if_not_exists=1 --query "
    SYSTEM ENABLE FAILPOINT file_read_inject_fixed_file_missing;
    ${query};
    SYSTEM DISABLE FAILPOINT file_read_inject_fixed_file_missing;
"

# With the setting off it is an error, and the same error the unsplit path reports. Only the code
# is printed: the message carries a path that differs every run.
"${LOCAL[@]}" "${split[@]}" --engine_file_empty_if_not_exists=0 --query "
    SYSTEM ENABLE FAILPOINT file_read_inject_fixed_file_missing;
    ${query};
" 2>&1 | grep -o -m1 'FILE_DOESNT_EXIST'
