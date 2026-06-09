#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/99326
# Avro output should throw BAD_ARGUMENTS instead of logical error
# when an Enum column contains a value not in the enum definition.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Inject an invalid enum value via RowBinary: binary deserialization of Enum
# is inherited from SerializationNumber and does NOT validate enum values.

# Test Enum8
${CLICKHOUSE_CLIENT} -q "CREATE TABLE enum8_wide (e Enum8('a' = 1, 'b' = 2, 'c' = 3)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "INSERT INTO enum8_wide VALUES ('c')"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE enum8_narrow (e Enum8('a' = 1, 'b' = 2)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM enum8_wide FORMAT RowBinary" | \
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO enum8_narrow FORMAT RowBinary"

# Avro writes a binary header to stdout before the serializer fails,
# so redirect stdout to /dev/null and capture stderr to a file.
${CLICKHOUSE_CLIENT} -q "SELECT * FROM enum8_narrow FORMAT Avro" > /dev/null 2> "${CLICKHOUSE_TMP}/avro_enum8_err.txt"
grep -q 'BAD_ARGUMENTS' "${CLICKHOUSE_TMP}/avro_enum8_err.txt" && echo "BAD_ARGUMENTS" || cat "${CLICKHOUSE_TMP}/avro_enum8_err.txt"

${CLICKHOUSE_CLIENT} -q "DROP TABLE enum8_wide"
${CLICKHOUSE_CLIENT} -q "DROP TABLE enum8_narrow"

# Test Enum16
${CLICKHOUSE_CLIENT} -q "CREATE TABLE enum16_wide (e Enum16('a' = 1, 'b' = 2, 'c' = 3)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "INSERT INTO enum16_wide VALUES ('c')"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE enum16_narrow (e Enum16('a' = 1, 'b' = 2)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM enum16_wide FORMAT RowBinary" | \
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO enum16_narrow FORMAT RowBinary"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM enum16_narrow FORMAT Avro" > /dev/null 2> "${CLICKHOUSE_TMP}/avro_enum16_err.txt"
grep -q 'BAD_ARGUMENTS' "${CLICKHOUSE_TMP}/avro_enum16_err.txt" && echo "BAD_ARGUMENTS" || cat "${CLICKHOUSE_TMP}/avro_enum16_err.txt"

${CLICKHOUSE_CLIENT} -q "DROP TABLE enum16_wide"
${CLICKHOUSE_CLIENT} -q "DROP TABLE enum16_narrow"
