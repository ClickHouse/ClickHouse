#!/usr/bin/env bash
# Tags: no-fasttest, long
# no-fasttest: needs the Parquet format which is not built in fasttest.
# long: writes a multi-row-group Parquet file so read tasks are still in flight on teardown.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A dictionary reading Parquet from a local file hands the ReadBuffer to the input format, which owns
# it. When the read throws, the pipeline releases that buffer on the way out while the format's
# background prefetch and decode tasks may still be reading through it. The error must surface every
# time and the server must stay alive; under a sanitizer build the buffer must not be read after
# release.

DICT="d_${CLICKHOUSE_DATABASE}"
# The dictionary FILE source needs an absolute path, and it must be the path this server actually
# serves -- ask the server rather than assuming a layout.
USER_FILES=$(${CLICKHOUSE_CLIENT} --query "select value from system.server_settings where name = 'user_files_path'")
REL="${CLICKHOUSE_DATABASE}/prefetch_lifetime.parquet"
ABS="${USER_FILES%/}/${REL}"

# Small row groups so there are many read ranges, hence many queued tasks at throw time.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${REL}', Parquet, 'key UInt64, val String')
    select number, repeat('y', 400) from numbers(200000)
    settings engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 5000,
             output_format_parquet_compression_method = 'none';
"

# `val` is Int64 in the dictionary but holds strings in the file, so the Parquet read throws
# mid-flight, which is what makes the pipeline tear down while tasks are still running.
${CLICKHOUSE_CLIENT} --query="
    create dictionary ${DICT} (key UInt64, val Int64) primary key key
    source(file(path '${ABS}' format 'Parquet'))
    layout(flat(max_array_size 500000)) lifetime(0);
"

# min_bytes_for_seek = 1 stops range coalescing, so each range becomes its own task.
for _ in 1 2 3 4 5; do
    ${CLICKHOUSE_CLIENT} \
        --max_download_threads=8 --max_parsing_threads=8 \
        --input_format_parquet_local_file_min_bytes_for_seek=1 \
        --query="select dictGet('${DICT}', 'val', toUInt64(5))" 2>&1 \
        | grep -c -m1 -F 'CANNOT_PARSE_TEXT'
done

# The server survived every attempt and still answers.
${CLICKHOUSE_CLIENT} --query="select 'alive', count() from system.dictionaries where database = currentDatabase() and name = '${DICT}'"

${CLICKHOUSE_CLIENT} --query="drop dictionary ${DICT}"
${CLICKHOUSE_CLIENT} --query="select * from file('${REL}', Parquet) where 0 format Null" 2>/dev/null
rm -f "${ABS}"
