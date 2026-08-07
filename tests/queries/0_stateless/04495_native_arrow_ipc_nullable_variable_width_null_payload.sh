#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The native Arrow IPC writer strips `Nullable` and hands the null map to the leaf encoder. The variable-width
# encoders -- the `String` path and the `output_format_arrow_unsupported_types_as_binary` fallback -- must emit
# a zero-length slot for a NULL row instead of appending the arbitrary nested bytes that row may still carry,
# matching the Apache Arrow library writer's `AppendNull`. `nullIf(x, x)` produces an all-NULL `Nullable`
# column whose nested column keeps its original (non-empty, distinct) values, exercising exactly that path.

TMP="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

NAT="output_format_arrow_use_native_writer = 1"
COMMON="output_format_arrow_string_as_string = 1, output_format_arrow_compression_method = 'none', engine_file_truncate_on_insert = 1"

# All NULL, nested carries a distinctive marker; a sibling column has the same NULL structure but trivial nested.
STR_MARK="SELECT nullIf('SECRETLEAK' || toString(number), 'SECRETLEAK' || toString(number))::Nullable(String) AS s FROM numbers(64)"
STR_PLAIN="SELECT nullIf(toString(number), toString(number))::Nullable(String) AS s FROM numbers(64)"

# `BFloat16` has no first-class Arrow mapping, so it goes through the raw-binary fallback.
BIN_VALS="SELECT nullIf(CAST(number + 1000 AS BFloat16), CAST(number + 1000 AS BFloat16))::Nullable(BFloat16) AS b FROM numbers(64)"
BIN_ZERO="SELECT nullIf(CAST(number * 0 AS BFloat16), CAST(number * 0 AS BFloat16))::Nullable(BFloat16) AS b FROM numbers(64)"

for FMT in Arrow ArrowStream; do
    echo "=== ${FMT} ==="

    # String: the nested bytes of NULL rows must not reach the native output.
    ${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file('${TMP}.s.${FMT}', '${FMT}') ${STR_MARK} SETTINGS ${NAT}, ${COMMON}"
    echo "string: leaked marker occurrences in native output: $(grep -c -a SECRETLEAK "${TMP}.s.${FMT}")"

    # The native output must not depend on what a NULL row carries in its nested column.
    ${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file('${TMP}.s2.${FMT}', '${FMT}') ${STR_PLAIN} SETTINGS ${NAT}, ${COMMON}"
    cmp -s "${TMP}.s.${FMT}" "${TMP}.s2.${FMT}" && echo "string: native output independent of NULL-row nested bytes: OK" || echo "string: native output independent of NULL-row nested bytes: MISMATCH"

    # Round-trips to all NULL, and the native reader agrees with the library reader.
    SN=$(${CLICKHOUSE_LOCAL} --query "SELECT count() = 64 AND countIf(s IS NULL) = 64 FROM file('${TMP}.s.${FMT}', '${FMT}') SETTINGS input_format_arrow_use_native_reader = 1")
    SL=$(${CLICKHOUSE_LOCAL} --query "SELECT count() = 64 AND countIf(s IS NULL) = 64 FROM file('${TMP}.s.${FMT}', '${FMT}') SETTINGS input_format_arrow_use_native_reader = 0")
    echo "string: round-trips to all NULL (native reader / library reader): ${SN} / ${SL}"

    # Binary fallback: same independence property and round-trip.
    ${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file('${TMP}.b.${FMT}', '${FMT}') ${BIN_VALS} SETTINGS ${NAT}, output_format_arrow_unsupported_types_as_binary = 1, ${COMMON}"
    ${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file('${TMP}.b2.${FMT}', '${FMT}') ${BIN_ZERO} SETTINGS ${NAT}, output_format_arrow_unsupported_types_as_binary = 1, ${COMMON}"
    cmp -s "${TMP}.b.${FMT}" "${TMP}.b2.${FMT}" && echo "binary: native output independent of NULL-row nested bytes: OK" || echo "binary: native output independent of NULL-row nested bytes: MISMATCH"
    BN=$(${CLICKHOUSE_LOCAL} --query "SELECT count() = 64 AND countIf(b IS NULL) = 64 FROM file('${TMP}.b.${FMT}', '${FMT}') SETTINGS input_format_arrow_use_native_reader = 1")
    echo "binary: round-trips to all NULL (native reader): ${BN}"

    rm -f "${TMP}.s.${FMT}" "${TMP}.s2.${FMT}" "${TMP}.b.${FMT}" "${TMP}.b2.${FMT}"
done
