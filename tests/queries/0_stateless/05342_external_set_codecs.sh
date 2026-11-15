#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-codecs.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

# The test checks which temporary files of a set `temporary_files_codec` compresses. A set of fixed-size keys
# compresses its runs with it, but a set never compresses its file. So the file of a set is the same with
# `LZ4` as with `NONE`, even for short codes in a `FixedString(16)`, which stay unpacked and whose padding
# fills most of their bytes. A set of `String` keys stores disk keys that are mostly hash bits, which compress
# too little to pay for a codec, so it writes its runs uncompressed too, whatever the codec is.
#
# With a threshold of 1 byte and one thread, each set spills before its first chunk and writes a run per
# chunk, so both processes write the same runs. Each set logs a trace line with the bytes of its file when it
# is written.
for codec in LZ4 NONE; do
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/${codec}" --config-file "${LOCAL_DIR}/query-log.yaml" \
        --max_bytes_before_external_set 1 --max_threads 1 --max_block_size 8192 --temporary_files_codec "${codec}" \
        --send_logs_level trace 2> "${LOCAL_DIR}/trace-${codec}.log" <<SQL
SELECT 'String', count() FROM numbers(50000) WHERE toString(number) IN (SELECT toString(number * 2) FROM numbers(30000))
SETTINGS log_comment = 'String';
SELECT 'UInt64', count() FROM numbers(50000) WHERE number IN (SELECT number * 2 FROM numbers(30000))
SETTINGS log_comment = 'UInt64';
SELECT 'FixedString(16)', count() FROM numbers(50000)
WHERE toFixedString(toString(number), 16) IN (SELECT toFixedString(toString(number * 2), 16) FROM numbers(30000))
SETTINGS log_comment = 'FixedString(16)';
SYSTEM FLUSH LOGS query_log;
SELECT 'all', '${codec}', log_comment, ProfileEvents['SetsSpilledToDisk'], ProfileEvents['ExternalSetCompressedBytes']
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment != '' AND current_database = currentDatabase()
ORDER BY event_time_microseconds
FORMAT TSV;
SQL
    paste <(printf 'file\t%s\tString\nfile\t%s\tUInt64\nfile\t%s\tFixedString(16)\n' "${codec}" "${codec}" "${codec}") \
        <(grep -o -E '[0-9]+ bytes in temporary file' "${LOCAL_DIR}/trace-${codec}.log" | grep -o -E '^[0-9]+')
done > "${LOCAL_DIR}/result.tsv"

# Both processes find the same keys, and every set spills. For each set, the report compares the bytes of all
# its temporary files with `LZ4` and with `NONE`, and then the bytes of its file alone.
grep -v -E '^(all|file)' "${LOCAL_DIR}/result.tsv"
awk -F'\t' -v OFS='\t' '
    function compare(a, b) { return a < b ? "<" : (a > b ? ">" : "=") }
    $1 == "all" { spilled[$3] += $4; all_bytes[$2, $3] = $5 }
    $1 == "file" { file_bytes[$2, $3] = $4 }
    END {
        split("String UInt64 FixedString(16)", names, " ")
        for (i = 1; i <= 3; ++i)
            print names[i], spilled[names[i]], compare(all_bytes["LZ4", names[i]], all_bytes["NONE", names[i]]),
                compare(file_bytes["LZ4", names[i]], file_bytes["NONE", names[i]])
    }' "${LOCAL_DIR}/result.tsv"
