#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# By-name formats such as `JSONEachRow` resolve field names through `CaseAwareBlockNameMap`, which honors
# `input_format_column_name_matching_mode` (`auto` by default: an exact-case match first, then a
# case-insensitive one). So a field `A` is matched to a column `a`. The schema-mismatch diagnostic must
# resolve names the same way: a plain exact lookup would treat `A` as an unknown field, silently drop it
# (because `input_format_skip_unknown_fields` defaults to 1) and fail to explain a mismatch that the real
# parser does detect.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- Field name differs only in case and its value has the wrong shape (a string for a numeric column): matched case-insensitively, so the mismatch is explained"
printf 'CREATE TABLE t (a UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow {"A":"not_a_number"}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- Field name differs only in case and its value is compatible (a number for a numeric column), only the value fails: no false positive"
printf 'CREATE TABLE t (a UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow {"A":1.5}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- With case-sensitive matching the field is genuinely unknown; the parser rejects it when input_format_skip_unknown_fields=0, so the differing structure is explained"
printf 'CREATE TABLE t (a UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow {"A":"not_a_number"}\n' \
    | $CLICKHOUSE_LOCAL --input_format_column_name_matching_mode match_case --input_format_skip_unknown_fields 0 2>&1 | check
