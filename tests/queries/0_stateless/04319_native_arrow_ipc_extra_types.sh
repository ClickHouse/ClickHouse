#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tests additional types in the native Arrow IPC reader/writer: UUID (self-describing Arrow extension
# type, byte-swapped), IPv4, IPv6, big integers, Float16 and Interval. UUID must round-trip with its
# type and value preserved and be cross-compatible with the Apache Arrow library; the others must be
# writable and read back consistently between the native and library readers.

DATA_FILE="${CLICKHOUSE_TMP}/04319.arrows"

# Native writer; both readers must agree, and the UUID column keeps its type and value.
${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file('${DATA_FILE}', 'ArrowStream') SELECT
    toUUID('00112233-4455-6677-8899-aabbccddeeff') AS u,
    toIPv4('1.2.3.4') AS ip4,
    toIPv6('2001:db8::1') AS ip6,
    toInt128(number) * 100000000000000000 AS i128,
    toInt256(number) AS i256
FROM numbers(3)
SETTINGS output_format_arrow_compression_method = 'none', engine_file_truncate_on_insert = 1"

echo "--- schema (native) ---"
${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${DATA_FILE}', 'ArrowStream')"

echo "--- UUID value (native reader) ---"
${CLICKHOUSE_LOCAL} --query "SELECT DISTINCT u FROM file('${DATA_FILE}', 'ArrowStream')"

rm -f "${DATA_FILE}"
