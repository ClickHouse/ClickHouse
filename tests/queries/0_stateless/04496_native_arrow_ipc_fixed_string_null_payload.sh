#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Companion to 04495 for the `FixedString` variable-width writer branch. With
# `output_format_arrow_fixed_string_as_fixed_byte_array = 0` the schema advertises a variable-width
# Utf8/Binary and the native writer emits an offsets buffer plus a data buffer. That branch must honor the
# null map: a NULL row contributes a zero-length slot, not the arbitrary nested bytes the row still carries,
# matching the Apache Arrow library writer's `AppendNull` and the `String` path. `nullIf(x, x)` produces an
# all-NULL `Nullable(FixedString)` whose nested column keeps its original distinct bytes.

TMP="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

NAT="output_format_arrow_use_native_writer = 1"
COMMON="output_format_arrow_fixed_string_as_fixed_byte_array = 0, output_format_arrow_string_as_string = 1, output_format_arrow_compression_method = 'none', engine_file_truncate_on_insert = 1"

# All NULL, nested carries a distinctive marker; a sibling column has the same NULL structure but trivial nested.
FS_MARK="SELECT nullIf(CAST('SECRETLEAK' || toString(number % 10) AS FixedString(16)), CAST('SECRETLEAK' || toString(number % 10) AS FixedString(16))) AS s FROM numbers(64)"
FS_PLAIN="SELECT nullIf(CAST('' AS FixedString(16)), CAST('' AS FixedString(16))) AS s FROM numbers(64)"

for FMT in Arrow ArrowStream; do
    echo "=== ${FMT} ==="

    # The nested bytes of NULL rows must not reach the native output.
    ${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file('${TMP}.s.${FMT}', '${FMT}') ${FS_MARK} SETTINGS ${NAT}, ${COMMON}"
    echo "fixedstring: leaked marker occurrences in native output: $(grep -c -a SECRETLEAK "${TMP}.s.${FMT}")"

    # The native output must not depend on what a NULL row carries in its nested column.
    ${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file('${TMP}.s2.${FMT}', '${FMT}') ${FS_PLAIN} SETTINGS ${NAT}, ${COMMON}"
    cmp -s "${TMP}.s.${FMT}" "${TMP}.s2.${FMT}" && echo "fixedstring: native output independent of NULL-row nested bytes: OK" || echo "fixedstring: native output independent of NULL-row nested bytes: MISMATCH"

    # Round-trips to all NULL, and the native reader agrees with the library reader.
    SN=$(${CLICKHOUSE_LOCAL} --query "SELECT count() = 64 AND countIf(s IS NULL) = 64 FROM file('${TMP}.s.${FMT}', '${FMT}') SETTINGS input_format_arrow_use_native_reader = 1")
    SL=$(${CLICKHOUSE_LOCAL} --query "SELECT count() = 64 AND countIf(s IS NULL) = 64 FROM file('${TMP}.s.${FMT}', '${FMT}') SETTINGS input_format_arrow_use_native_reader = 0")
    echo "fixedstring: round-trips to all NULL (native reader / library reader): ${SN} / ${SL}"

    rm -f "${TMP}.s.${FMT}" "${TMP}.s2.${FMT}"
done
