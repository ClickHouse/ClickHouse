#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tests `output_format_arrow_record_batch_size` and `output_format_arrow_record_batch_size_bytes`, which
# combine consecutive blocks that are individually smaller than the target into one Arrow IPC record batch.
# Every arm asserts an exact batch count plus the rows read back, so that combining cannot lose, duplicate
# or reorder rows.
#
# `numbers` (never `numbers_mt`) with `max_block_size` pinned in the query's own SETTINGS, which beats
# the test runner's randomization, is what makes the sequence of blocks reaching the writer, and therefore
# the batch counts, reproducible. `max_threads = 1` keeps the source and the read-back single-stream, so
# that neither the batch counts nor the `in_order` check below can depend on scheduling.
#
# The result is written with `FORMAT ArrowStream` redirected to a file, never with
# `INSERT INTO FUNCTION file(...)`: `ArrowStream` prefers large blocks, so the INSERT path would squash to
# `min_insert_block_size_rows` and deliver the whole fixture as one block, leaving every arm at one batch
# whether or not the feature works.

FILE="${CLICKHOUSE_TMP}/05211_arrow_batch_coalescing.arrows"

# An older pyarrow cannot read the sentinel written for an uncompressed lz4_frame body, and the batch
# counts must not depend on the reader's codec support.
COMMON="max_threads = 1, output_format_arrow_compression_method = 'none'"

# Prints the number of record batches, and with a second argument also the rows in each of them.
arrow_batches() {
    python3 - "$FILE" "$1" "${2-}" <<'PY'
import sys
import pyarrow as pa

path, mode, detail = sys.argv[1], sys.argv[2], sys.argv[3]
with pa.OSFile(path, "rb") as source:
    reader = pa.ipc.open_file(source) if mode == "file" else pa.ipc.open_stream(source)
    if mode == "file":
        rows = [reader.get_batch(i).num_rows for i in range(reader.num_record_batches)]
    else:
        rows = [batch.num_rows for batch in reader]
out = "batches: %d" % len(rows)
if detail:
    out += ", rows per batch: %s" % rows
print(out)
PY
}

# The count and the checksum prove no row was lost or duplicated; `in_order` proves that combining
# appended rows instead of reordering them, since every fixture below is produced in ascending order.
oracle() {
    ${CLICKHOUSE_LOCAL} --query "SELECT count(), sum(number), groupArray(number) = arraySort(groupArray(number)) AS in_order FROM file('${FILE}', '${1-ArrowStream}') SETTINGS max_threads = 1"
}

# The same, for the `LowCardinality(String)` arms whose fixture is not a sequence of numbers: the row count,
# the distinct values and their total length prove the combined batch still describes every row.
string_oracle() {
    ${CLICKHOUSE_LOCAL} --query "SELECT count(), uniqExact(s), sum(length(s)) FROM file('${FILE}', 'ArrowStream') SETTINGS max_threads = 1"
}

echo "--- one batch per block by default ---"
${CLICKHOUSE_LOCAL} --query "SELECT number FROM numbers(64) SETTINGS max_block_size = 1, ${COMMON} FORMAT ArrowStream" > "${FILE}"
arrow_batches stream
oracle

echo "--- both targets set: a result below both leaves as one batch ---"
${CLICKHOUSE_LOCAL} --query "SELECT number FROM numbers(64) SETTINGS max_block_size = 1, ${COMMON}, output_format_arrow_record_batch_size = 65409, output_format_arrow_record_batch_size_bytes = 1048576 FORMAT ArrowStream" > "${FILE}"
arrow_batches stream
oracle

echo "--- row target alone: 64 rows in batches of 16 ---"
${CLICKHOUSE_LOCAL} --query "SELECT number FROM numbers(64) SETTINGS max_block_size = 1, ${COMMON}, output_format_arrow_record_batch_size = 16, output_format_arrow_record_batch_size_bytes = 0 FORMAT ArrowStream" > "${FILE}"
arrow_batches stream
oracle

echo "--- byte target alone: 8 bytes per row, so batches of 8 rows ---"
${CLICKHOUSE_LOCAL} --query "SELECT number FROM numbers(64) SETTINGS max_block_size = 1, ${COMMON}, output_format_arrow_record_batch_size = 0, output_format_arrow_record_batch_size_bytes = 64 FORMAT ArrowStream" > "${FILE}"
arrow_batches stream
oracle

echo "--- a combined batch may exceed the target: two 40-row blocks under a 41-row target ---"
${CLICKHOUSE_LOCAL} --query "SELECT number FROM numbers(80) SETTINGS max_block_size = 40, ${COMMON}, output_format_arrow_record_batch_size = 41, output_format_arrow_record_batch_size_bytes = 0 FORMAT ArrowStream" > "${FILE}"
arrow_batches stream detail
oracle

echo "--- blocks that already reach the target are neither merged nor split ---"
${CLICKHOUSE_LOCAL} --query "SELECT number FROM numbers(200000) SETTINGS max_block_size = 65409, ${COMMON}, output_format_arrow_record_batch_size = 65409, output_format_arrow_record_batch_size_bytes = 0 FORMAT ArrowStream" > "${FILE}"
arrow_batches stream detail
oracle

echo "--- a block reaching the target flushes what is staged instead of absorbing it ---"
# The three blocks are 3, 65409 and 254 rows. `toString(number) IN (...)` rather than `number < 3` keeps
# the condition out of reach of the range analysis of `numbers`, which would otherwise generate exactly
# the matching rows and deliver them as two full blocks, leaving no small block in front of the big one.
${CLICKHOUSE_LOCAL} --query "SELECT number FROM numbers(131072) WHERE toString(number) IN ('0', '1', '2') OR number > 65408 SETTINGS max_block_size = 65409, ${COMMON}, output_format_arrow_record_batch_size = 1000, output_format_arrow_record_batch_size_bytes = 0 FORMAT ArrowStream" > "${FILE}"
arrow_batches stream detail
oracle

echo "--- a multi-column result keeps every value, in order, when combined ---"
${CLICKHOUSE_LOCAL} --query "SELECT 7 AS c, toLowCardinality(toString(number % 3)) AS lc, if(number % 4 = 0, NULL, number)::Nullable(UInt64) AS n FROM numbers(8) SETTINGS max_block_size = 1, ${COMMON}, output_format_arrow_record_batch_size = 65409 FORMAT ArrowStream" > "${FILE}"
arrow_batches stream
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${FILE}', 'ArrowStream') SETTINGS max_threads = 1"

echo "--- dictionary-encoded LowCardinality spread over several combined batches ---"
# Every batch after the first mixes a value the stream's dictionary already carries with one it does not, so
# each emits a dictionary delta and has the indexes of a merged column remapped against the accumulated dictionary.
DICT_FIXTURE="toLowCardinality(if(number % 4 < 2, 'shared', toString(intDiv(number, 4)))) AS lc FROM numbers(16)"
DICT_READBACK="SELECT lc, count() FROM file('${FILE}', 'ArrowStream') GROUP BY lc ORDER BY lc SETTINGS max_threads = 1"
${CLICKHOUSE_LOCAL} --query "SELECT ${DICT_FIXTURE} SETTINGS max_block_size = 1, ${COMMON}, output_format_arrow_low_cardinality_as_dictionary = 1, output_format_arrow_record_batch_size = 4 FORMAT ArrowStream" > "${FILE}"
arrow_batches stream detail
${CLICKHOUSE_LOCAL} --query "${DICT_READBACK}"

echo "--- the same dictionary column combined into one batch ---"
${CLICKHOUSE_LOCAL} --query "SELECT ${DICT_FIXTURE} SETTINGS max_block_size = 1, ${COMMON}, output_format_arrow_low_cardinality_as_dictionary = 1, output_format_arrow_record_batch_size = 65409 FORMAT ArrowStream" > "${FILE}"
arrow_batches stream detail
${CLICKHOUSE_LOCAL} --query "${DICT_READBACK}"

echo "--- the byte target counts the bytes the block holds, which for LowCardinality is a deduplicated value ---"
# The byte target counts one index per row plus the dictionary once (`ColumnLowCardinality::byteSize`), while
# the encoder writes one value per row: documented on the setting, hence asserted here rather than filed as a bug.
${CLICKHOUSE_LOCAL} --query "SELECT toLowCardinality(repeat('x', 1000)) AS s FROM numbers(100) SETTINGS max_block_size = 1, ${COMMON}, output_format_arrow_low_cardinality_as_dictionary = 0, output_format_arrow_record_batch_size = 0, output_format_arrow_record_batch_size_bytes = 1500 FORMAT ArrowStream" > "${FILE}"
arrow_batches stream detail
string_oracle
${CLICKHOUSE_LOCAL} --query "SELECT repeat('x', 1000) AS s FROM numbers(100) SETTINGS max_block_size = 1, ${COMMON}, output_format_arrow_record_batch_size = 0, output_format_arrow_record_batch_size_bytes = 1500 FORMAT ArrowStream" > "${FILE}"
arrow_batches stream

echo "--- the row criterion still applies when the byte criterion is set, and bounds that batch ---"
${CLICKHOUSE_LOCAL} --query "SELECT toLowCardinality(repeat('x', 1000)) AS s FROM numbers(100) SETTINGS max_block_size = 1, ${COMMON}, output_format_arrow_low_cardinality_as_dictionary = 0, output_format_arrow_record_batch_size = 8, output_format_arrow_record_batch_size_bytes = 1500 FORMAT ArrowStream" > "${FILE}"
arrow_batches stream detail
string_oracle

echo "--- a block keeping the dictionary of the block it was filtered out of reaches the byte target alone ---"
# `ColumnLowCardinality::filter` keeps the whole source dictionary, so each surviving row measures the
# ~64 KB dictionary its 250-row block built and is written as its own record batch, which also ends the
# batches the row target set here would otherwise have combined. The `OR` is what keeps the filter above
# the projection that builds `s`, so the dictionary is built for the whole block; `length(s) = 0` is
# never true. The `String` arm below has the same values and the same targets and is combined.
LC_SOURCE_DICT="SELECT s FROM (SELECT toLowCardinality(repeat(toString(number), 100)) AS s, number AS n FROM numbers(1000)) WHERE (n % 250 = 0) OR (length(s) = 0)"
${CLICKHOUSE_LOCAL} --query "${LC_SOURCE_DICT} SETTINGS max_block_size = 250, ${COMMON}, output_format_arrow_low_cardinality_as_dictionary = 0, output_format_arrow_record_batch_size = 65409, output_format_arrow_record_batch_size_bytes = 8192 FORMAT ArrowStream" > "${FILE}"
arrow_batches stream detail
string_oracle
${CLICKHOUSE_LOCAL} --query "SELECT s FROM (SELECT repeat(toString(number), 100) AS s, number AS n FROM numbers(1000)) WHERE (n % 250 = 0) OR (length(s) = 0) SETTINGS max_block_size = 250, ${COMMON}, output_format_arrow_record_batch_size = 65409, output_format_arrow_record_batch_size_bytes = 8192 FORMAT ArrowStream" > "${FILE}"
arrow_batches stream detail

echo "--- the row target alone combines those same blocks ---"
${CLICKHOUSE_LOCAL} --query "${LC_SOURCE_DICT} SETTINGS max_block_size = 250, ${COMMON}, output_format_arrow_low_cardinality_as_dictionary = 0, output_format_arrow_record_batch_size = 65409, output_format_arrow_record_batch_size_bytes = 0 FORMAT ArrowStream" > "${FILE}"
arrow_batches stream detail
string_oracle

echo "--- the Arrow file format coalesces too, and its footer stays consistent ---"
${CLICKHOUSE_LOCAL} --query "SELECT number FROM numbers(64) SETTINGS max_block_size = 1, ${COMMON}, output_format_arrow_record_batch_size = 65409 FORMAT Arrow" > "${FILE}"
arrow_batches file
oracle Arrow

echo "--- an Arrow file footer with one Block per combined batch ---"
${CLICKHOUSE_LOCAL} --query "SELECT number FROM numbers(64) SETTINGS max_block_size = 1, ${COMMON}, output_format_arrow_record_batch_size = 16 FORMAT Arrow" > "${FILE}"
arrow_batches file detail
oracle Arrow

rm -f "${FILE}"
