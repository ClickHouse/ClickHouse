#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A field in the input that is unknown to the destination is only ignorable when the parser really skips
# it. `input_format_json_ignore_unnecessary_fields` (enabled by default) does NOT make an unknown field
# ignorable on its own: `JSONEachRowRowInputFormat` still routes it through `skipUnknownField`, which
# throws `INCORRECT_DATA` when `input_format_skip_unknown_fields = 0` (verified with a valid value:
# `{"a":1,"extra":"x"}` -> Code 117 `Unknown field found ... extra`). So with `skip_unknown_fields = 0` an
# extra field is a genuine structure difference the parser rejects, and the diagnostic should explain it;
# with `skip_unknown_fields = 1` the extra field is legally skipped, so an unrelated value error must not
# pick up a misleading structure-mismatch suffix.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- ignore_unnecessary_fields=1, skip_unknown_fields=0: the extra field is rejected by the parser, so the differing structure is explained"
printf 'CREATE TABLE t (a UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow {"a":1.5,"extra":"x"}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_ignore_unnecessary_fields 1 --input_format_skip_unknown_fields 0 2>&1 | check

echo "-- ignore_unnecessary_fields=1, skip_unknown_fields=1 (default): the extra field is legally skipped, only the value fails (no false positive)"
printf 'CREATE TABLE t (a UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow {"a":1.5,"extra":"x"}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_ignore_unnecessary_fields 1 --input_format_skip_unknown_fields 1 2>&1 | check
