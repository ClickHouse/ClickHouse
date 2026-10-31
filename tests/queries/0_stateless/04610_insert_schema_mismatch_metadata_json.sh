#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The metadata-based JSON formats (`JSON`, `JSONCompact`, `JSONColumnsWithMetadata`) declare column
# types in a `meta` section, but the parser only validates them against the destination exactly when
# `input_format_json_validate_types_from_metadata` is enabled. With it disabled the parser ignores the
# declared types and reads the data by value (and positionally for `JSONCompact`), so the schema read
# during inference (which reflects `meta`) no longer describes what is parsed. The schema-mismatch
# explanation must follow the real parser: an exact comparison when validation is on, and no comparison
# at all when it is off, otherwise it would wrongly flag (or wrongly hide) a mismatch.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- JSONColumnsWithMetadata, validation off: metadata types are ignored, only a value fails (no false positive)"
printf 'CREATE TABLE t (x UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONColumnsWithMetadata {"meta":[{"name":"x","type":"String"}],"data":{"x":[1.5]}}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_validate_types_from_metadata 0 2>&1 | check

echo "-- JSONColumnsWithMetadata, validation on: declared type differs from the destination, exact check applies"
printf 'CREATE TABLE t (x UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONColumnsWithMetadata {"meta":[{"name":"x","type":"UInt64"}],"data":{"x":[5]}}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_validate_types_from_metadata 1 2>&1 | check

echo "-- JSONColumnsWithMetadata, validation on: declared type matches the destination, only a value fails (no false positive)"
printf 'CREATE TABLE t (x UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONColumnsWithMetadata {"meta":[{"name":"x","type":"UInt8"}],"data":{"x":[1.5]}}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_validate_types_from_metadata 1 2>&1 | check

echo "-- JSONCompact, validation off: metadata is ignored and rows are read positionally (no false positive)"
printf 'CREATE TABLE t (x UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONCompact {"meta":[{"name":"x","type":"String"}],"data":[[1.5]]}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_validate_types_from_metadata 0 2>&1 | check

echo "-- JSON without a metadata section: falls back to JSONEachRow inference, a genuine mismatch is still explained"
printf 'CREATE TABLE t (x UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSON {"x":"not_a_number"}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check
