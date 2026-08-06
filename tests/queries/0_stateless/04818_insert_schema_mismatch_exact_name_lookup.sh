#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# Not every by-name parser honors `input_format_column_name_matching_mode`: `TSKV` looks field names
# up in a plain hash map, exactly, so a case-only difference (`A=...` into a column `a`) is a genuine
# unknown field the parser rejects when `input_format_skip_unknown_fields` = 0. The schema-mismatch
# diagnostic must resolve names the same way — treating `A` as a match for `a` there would suppress
# the explanation of a mismatch the parser detects. The `JSONEachRow` parser, by contrast, does
# resolve names case-aware, and the diagnostic must keep doing the same for it (see also 04612).

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- TSKV, case-only field name difference with skip_unknown_fields = 0: the parser rejects the unknown field, and the differing structure is explained"
printf 'A=1\tb=1\n' | $CLICKHOUSE_LOCAL --input_format_skip_unknown_fields 0 -q "
    CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory;
    INSERT INTO t FORMAT TSKV
" 2>&1 | check

echo "-- TSKV, exact field names, a value-level error only: no false positive"
printf 'a=1.5\tb=1\n' | $CLICKHOUSE_LOCAL -q "
    CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory;
    INSERT INTO t FORMAT TSKV
" 2>&1 | check

echo "-- JSONEachRow resolves names case-aware: a genuine mismatch under a case-only name difference is still explained"
printf '{"A":"text"}\n' | $CLICKHOUSE_LOCAL -q "
    CREATE TABLE t (a UInt8) ENGINE = Memory;
    INSERT INTO t FORMAT JSONEachRow
" 2>&1 | check
