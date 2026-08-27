#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A dictionary reading Parquet from a local file hands the ReadBuffer to the input format, which owns
# it. When the read throws, the pipeline releases that buffer on the way out while the format's
# background prefetch and decode tasks may still be reading through it. The error must surface every
# time and the server must stay alive; under a sanitizer build the buffer must not be read after
# release.

DICT="d_${CLICKHOUSE_DATABASE}"
REL="${CLICKHOUSE_DATABASE}/prefetch_lifetime.parquet"

# Small row groups so there are many read ranges, hence many queued tasks at throw time.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${REL}', Parquet, 'key UInt64, val String')
    select number, repeat('y', 400) from numbers(2000000)
    settings engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 5000,
             output_format_parquet_compression_method = 'none';
"

# The dictionary FILE source needs the actual absolute path. The `user_files_path` setting can be
# empty, so derive the resolved path from the table function instead of composing it from that
# setting.
ABS=$(${CLICKHOUSE_CLIENT} --query="select _path from file('${REL}', Parquet) limit 1")

# `val` is Int64 in the dictionary but holds strings in the file, so the Parquet read throws
# mid-flight, which is what makes the pipeline tear down while tasks are still running.
#
# The settings have to be on the dictionary, not on the query: a dictionary loads in the global
# context, and the `file` source only picks up this SETTINGS clause. min_bytes_for_seek = 1 stops
# range coalescing, so each range becomes its own task, and the prefetch pool has to exist for those
# tasks to run in the background rather than inline on the decoding thread.
${CLICKHOUSE_CLIENT} --query="
    create dictionary ${DICT} (key UInt64, val Int64) primary key key
    source(file(path '${ABS}' format 'Parquet'))
    layout(flat(max_array_size 5000000)) lifetime(0)
    settings(max_download_threads = 32, max_parsing_threads = 32,
             input_format_parquet_use_native_reader_v3 = 1,
             input_format_parquet_local_file_min_bytes_for_seek = 1,
             input_format_parquet_enable_row_group_prefetch = 1);
"

# A forced reload, because a plain dictGet would replay the first load's cached exception instead of
# reading the file again.
for _ in 1 2 3 4 5; do
    ${CLICKHOUSE_CLIENT} --log_comment="${DICT}_reload" --query="system reload dictionary ${DICT}" 2>&1 \
        | grep -c -m1 -F 'CANNOT_PARSE_TEXT'
done

# The server survived every attempt and still answers.
${CLICKHOUSE_CLIENT} --query="select 'alive', count() from system.dictionaries where database = currentDatabase() and name = '${DICT}'"

${CLICKHOUSE_CLIENT} --query="drop dictionary ${DICT}"
${CLICKHOUSE_CLIENT} --query="select * from file('${REL}', Parquet) where 0 format Null" 2>/dev/null
rm -f "${ABS}"
