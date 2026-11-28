#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `Arrow` and `ORC` formats are not available in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/pull/110626
# Most formats that map columns by name fill a destination column absent from the data with defaults,
# so a missing column is not a structure mismatch. But `Arrow` and `ORC` reject a missing column when
# `input_format_arrow_allow_missing_columns` / `input_format_orc_allow_missing_columns` is disabled,
# so there the missing column is part of the mismatch. The sampled rows of the present column `x` are
# valid numeric text; a later row fails the cast with `CANNOT_PARSE_TEXT`.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

for format in Arrow ORC; do
    setting="input_format_$(echo "$format" | tr '[:upper:]' '[:lower:]')_allow_missing_columns"
    data="$CLICKHOUSE_TMP/data_05385.$format"
    $CLICKHOUSE_LOCAL -q "SELECT if(number < 5, '1', 'x') AS x FROM numbers(10) FORMAT $format" > "$data"
    for allow in 1 0; do
        echo "-- $format, $setting = $allow"
        {
            echo "SET $setting = $allow, input_format_max_rows_to_read_for_schema_inference = 2;"
            echo "CREATE TABLE t (x UInt8, y UInt8) ENGINE = Memory; INSERT INTO t FORMAT $format"
            cat "$data"
        } | $CLICKHOUSE_LOCAL 2>&1 | check
    done
    rm -f "$data"
done
