#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-disk-key-order.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

# A set on disk stores its keys in the ascending order of its disk keys. With a threshold of 1 byte, each set
# spills before its first chunk and, in the `break` mode, keeps the merged blocks of 50 keys up to the 100th:
# the 100 smallest keys when the disk keys keep the order of the keys, which arrive scattered. Each query
# counts the probed keys that the set finds and those of them among the 100 smallest, so it shows both which
# keys the set keeps and that it finds them. The cases cover every order of a column (unsigned, signed,
# floating-point and bytes) at each width, the methods of fixed-size keys, a packing of columns by size that
# differs from their order, and nullable keys, where `NULL` follows the values of its column.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --config-file "${LOCAL_DIR}/query-log.yaml" \
    --max_bytes_before_external_set 1 --max_rows_in_set 100 --set_overflow_mode break --max_block_size 50 <<'SQL'
SELECT 'Int8', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, toInt8(i - 128)
    IN (SELECT toInt8(n - 128) FROM (SELECT number * 151 % 256 AS n FROM numbers(256))) AS found FROM numbers(256))
SETTINGS log_comment = 'Int8';
SELECT 'Int64', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, toInt64(i - 500) * 1000000000
    IN (SELECT toInt64(n - 500) * 1000000000 FROM (SELECT number * 7919 % 1000 AS n FROM numbers(1000))) AS found FROM numbers(1000))
SETTINGS log_comment = 'Int64';
SELECT 'Int128', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, toInt128(i) - 500
    IN (SELECT toInt128(n) - 500 FROM (SELECT number * 7919 % 1000 AS n FROM numbers(1000))) AS found FROM numbers(1000))
SETTINGS log_comment = 'Int128';
SELECT 'Int256', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, toInt256(i) - 500
    IN (SELECT toInt256(n) - 500 FROM (SELECT number * 7919 % 1000 AS n FROM numbers(1000))) AS found FROM numbers(1000))
SETTINGS log_comment = 'Int256';
SELECT 'Decimal32', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, toDecimal32(toInt32(i) - 500, 2)
    IN (SELECT toDecimal32(toInt32(n) - 500, 2) FROM (SELECT number * 7919 % 1000 AS n FROM numbers(1000))) AS found FROM numbers(1000))
SETTINGS log_comment = 'Decimal32';
SELECT 'BFloat16', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, toBFloat16(toInt32(i) - 128)
    IN (SELECT toBFloat16(toInt32(n) - 128) FROM (SELECT number * 151 % 256 AS n FROM numbers(256))) AS found FROM numbers(256))
SETTINGS log_comment = 'BFloat16';
SELECT 'Float32', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, toFloat32((toInt32(i) - 500) / 8)
    IN (SELECT toFloat32((toInt32(n) - 500) / 8) FROM (SELECT number * 7919 % 1000 AS n FROM numbers(1000))) AS found FROM numbers(1000))
SETTINGS log_comment = 'Float32';
SELECT 'Float64', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, (toInt32(i) - 500) / 7
    IN (SELECT (toInt32(n) - 500) / 7 FROM (SELECT number * 7919 % 1000 AS n FROM numbers(1000))) AS found FROM numbers(1000))
SETTINGS log_comment = 'Float64';
SELECT 'UUID', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, reinterpret(bitShiftLeft(toUInt128(i), 64) + toUInt128(999 - i), 'UUID')
    IN (SELECT reinterpret(bitShiftLeft(toUInt128(n), 64) + toUInt128(999 - n), 'UUID') FROM (SELECT number * 7919 % 1000 AS n FROM numbers(1000))) AS found FROM numbers(1000))
SETTINGS log_comment = 'UUID';
SELECT 'UInt256', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, bitShiftLeft(toUInt256(i), 192) + toUInt256(999 - i)
    IN (SELECT bitShiftLeft(toUInt256(n), 192) + toUInt256(999 - n) FROM (SELECT number * 7919 % 1000 AS n FROM numbers(1000))) AS found FROM numbers(1000))
SETTINGS log_comment = 'UInt256';
SELECT 'FixedString(3)', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, toFixedString(leftPad(toString(i), 3, '0'), 3)
    IN (SELECT toFixedString(leftPad(toString(n), 3, '0'), 3) FROM (SELECT number * 7919 % 1000 AS n FROM numbers(1000))) AS found FROM numbers(1000))
SETTINGS log_comment = 'FixedString(3)';
SELECT 'FixedString(32)', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, toFixedString(leftPad(toString(i), 32, '0'), 32)
    IN (SELECT toFixedString(leftPad(toString(n), 32, '0'), 32) FROM (SELECT number * 7919 % 1000 AS n FROM numbers(1000))) AS found FROM numbers(1000))
SETTINGS log_comment = 'FixedString(32)';
SELECT 'IPv6', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, toIPv6(concat('2001:db8::', hex(toUInt16(i))))
    IN (SELECT toIPv6(concat('2001:db8::', hex(toUInt16(n)))) FROM (SELECT number * 7919 % 1000 AS n FROM numbers(1000))) AS found FROM numbers(1000))
SETTINGS log_comment = 'IPv6';
SELECT '(UInt32, UInt32)', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, (toUInt32(intDiv(i, 10)), toUInt32(i % 10))
    IN (SELECT toUInt32(intDiv(n, 10)), toUInt32(n % 10) FROM (SELECT number * 7919 % 1000 AS n FROM numbers(1000))) AS found FROM numbers(1000))
SETTINGS log_comment = '(UInt32, UInt32)';
SELECT '(UInt64, UInt8)', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, (toUInt64(intDiv(i, 10)), toUInt8(i % 10))
    IN (SELECT toUInt64(intDiv(n, 10)), toUInt8(n % 10) FROM (SELECT number * 7919 % 1000 AS n FROM numbers(1000))) AS found FROM numbers(1000))
SETTINGS log_comment = '(UInt64, UInt8)';
SELECT '(Int16, Float32)', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, (toInt16(toInt32(intDiv(i, 10)) - 50), toFloat32(toInt32(i % 10) - 5))
    IN (SELECT toInt16(toInt32(intDiv(n, 10)) - 50), toFloat32(toInt32(n % 10) - 5) FROM (SELECT number * 7919 % 1000 AS n FROM numbers(1000))) AS found FROM numbers(1000))
SETTINGS log_comment = '(Int16, Float32)';
-- Keys with `NULL` in different columns: `(NULL, 0)` and `(i, NULL)` follow the keys `(i, j)`, and
-- `(NULL, 0)` follows all keys `(i, NULL)`, as in `ORDER BY`.
SELECT '(Nullable(Int16), Nullable(UInt16))', countIf(found), countIf(found AND i < 112 AND i % 10 != 0)
FROM (SELECT number AS i, (if(i % 10 = 0, NULL, toInt16(toInt32(intDiv(i, 10)) - 50)), if(i % 10 = 5, NULL, toUInt16(i % 10)))
    IN (SELECT if(n % 10 = 0, NULL, toInt16(toInt32(intDiv(n, 10)) - 50)), if(n % 10 = 5, NULL, toUInt16(n % 10)) FROM (SELECT number * 7919 % 1000 AS n FROM numbers(1000))) AS found FROM numbers(1000))
SETTINGS transform_null_in = 1, log_comment = '(Nullable(Int16), Nullable(UInt16))';

-- The disk keys of floating-point numbers keep apart the values that differ in their bits, as the set in
-- memory does: `0.` and `-0.`, and `NaN` values with different payloads. The probes are sorted by their bits.
SELECT 'Float64 bits', groupArray(found)
FROM (SELECT b, reinterpretAsFloat64(b) IN (SELECT reinterpretAsFloat64(arrayJoin(
        [toUInt64(0), 9223372036854775808, 9221120237041090561, 18444492273895866369, 4607182418800017408]))) AS found
    FROM (SELECT arrayJoin([toUInt64(0), 9223372036854775808, 9221120237041090561, 9221120237041090562,
        18444492273895866369, 18444492273895866370, 4607182418800017408]) AS b) ORDER BY b)
SETTINGS max_rows_in_set = 0, log_comment = 'Float64 bits';

-- Every set spilled.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['SetsSpilledToDisk']
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment != '' AND current_database = currentDatabase()
ORDER BY event_time_microseconds;
SQL
