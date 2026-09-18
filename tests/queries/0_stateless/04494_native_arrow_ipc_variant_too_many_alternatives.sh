#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An Arrow dense-union type id is a signed int8 and the trailing NULL child uses id `num_variants`, so a
# `Variant` with more than 127 alternatives cannot be represented. The native Arrow IPC writer must reject
# it during schema construction, so an *empty* result - which writes the schema but never encodes a record
# batch - is rejected too, instead of silently emitting a union schema with out-of-range type ids.

# `FixedString(N)` of distinct widths gives 128 (and 127) distinct alternatives; they are "suspicious"
# (similar types) so allow them explicitly.
V128=$(seq 1 128 | sed 's/^/FixedString(/; s/$/)/' | paste -sd, -)
V127=$(seq 1 127 | sed 's/^/FixedString(/; s/$/)/' | paste -sd, -)
SUS="allow_suspicious_variant_types = 1"

# Empty result with 128 alternatives: only the schema is written, but it must still be rejected.
echo "--- empty Arrow, Variant(128): rejected ---"
${CLICKHOUSE_LOCAL} --query "SELECT CAST(NULL, 'Variant(${V128})') AS v FROM numbers(0) FORMAT Arrow SETTINGS output_format_arrow_use_native_writer = 1, ${SUS}" 2>&1 1>/dev/null | grep -oF "Native Arrow IPC writer does not support a Variant with 128 alternatives" | head -1

echo "--- empty ArrowStream, Variant(128): rejected ---"
${CLICKHOUSE_LOCAL} --query "SELECT CAST(NULL, 'Variant(${V128})') AS v FROM numbers(0) FORMAT ArrowStream SETTINGS output_format_arrow_use_native_writer = 1, ${SUS}" 2>&1 1>/dev/null | grep -oF "Native Arrow IPC writer does not support a Variant with 128 alternatives" | head -1

# Non-empty result with 128 alternatives: rejected at the encoder too.
echo "--- non-empty Arrow, Variant(128): rejected ---"
${CLICKHOUSE_LOCAL} --query "SELECT CAST(NULL, 'Variant(${V128})') AS v FROM numbers(3) FORMAT Arrow SETTINGS output_format_arrow_use_native_writer = 1, ${SUS}" 2>&1 1>/dev/null | grep -oF "Native Arrow IPC writer does not support a Variant with 128 alternatives" | head -1

# 127 alternatives is the maximum and must be written successfully (boundary), for both empty and non-empty.
echo "--- empty ArrowStream, Variant(127): accepted ---"
${CLICKHOUSE_LOCAL} --query "SELECT CAST(NULL, 'Variant(${V127})') AS v FROM numbers(0) FORMAT ArrowStream SETTINGS output_format_arrow_use_native_writer = 1, ${SUS}" > /dev/null 2>&1 && echo "OK" || echo "FAIL"

echo "--- non-empty ArrowStream, Variant(127): accepted ---"
${CLICKHOUSE_LOCAL} --query "SELECT CAST(NULL, 'Variant(${V127})') AS v FROM numbers(3) FORMAT ArrowStream SETTINGS output_format_arrow_use_native_writer = 1, ${SUS}" > /dev/null 2>&1 && echo "OK" || echo "FAIL"
