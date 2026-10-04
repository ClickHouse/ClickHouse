#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Arrow support is not compiled into the fast-test build.

# A self-round-trip of nested `UUID2` values (inside `Array`, `Tuple`, `Map`, `Nullable` and
# `LowCardinality`) without an explicit schema must restore the exact `UUID2` type in every
# writer/reader combination, including the Apache Arrow library reader/writer. The writers attach the
# ClickHouse-specific discriminator (`ClickHouse:type` = `UUID2`) to the leaf fields, and the readers
# consult the child field during the list/struct/map recursion. Sibling plain `UUID` leaves carry no
# discriminator and keep reading back as `UUID`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

u="61f0c404-5cb3-11e7-907b-a6006ad3dba0"

echo "-- Arrow nested, all writer/reader combinations"
for w in 1 0
do
    for r in 1 0
    do
        echo "native_writer=$w native_reader=$r:"
        $CLICKHOUSE_LOCAL -q "
            SELECT
                ['$u'::UUID2] AS arr,
                map('$u'::UUID2, '$u'::UUID) AS m,
                tuple('$u'::UUID2, '$u'::UUID)::Tuple(a UUID2, b UUID) AS t,
                [NULL, '$u'::UUID2]::Array(Nullable(UUID2)) AS an,
                [tuple('$u'::UUID2, '$u'::UUID)]::Array(Tuple(a UUID2, b UUID)) AS deep
            SETTINGS output_format_arrow_use_native_writer = $w FORMAT Arrow" \
            | $CLICKHOUSE_LOCAL --input-format Arrow --input_format_arrow_use_native_reader "$r" \
                -q "SELECT toTypeName(arr), toTypeName(m), toTypeName(t), toTypeName(an), toTypeName(deep), toString(arr[1]), toString(an[2]), toString(deep[1].a) FROM table"
    done
done

echo "-- Arrow LowCardinality (plain values), all writer/reader combinations"
for w in 1 0
do
    for r in 1 0
    do
        echo -n "native_writer=$w native_reader=$r: "
        $CLICKHOUSE_LOCAL -q "SELECT toLowCardinality('$u'::UUID2) AS lc SETTINGS output_format_arrow_use_native_writer = $w, allow_suspicious_low_cardinality_types = 1 FORMAT Arrow" \
            | $CLICKHOUSE_LOCAL --input-format Arrow --input_format_arrow_use_native_reader "$r" \
                -q "SELECT toTypeName(lc), toString(lc) FROM table"
    done
done

echo "-- Arrow LowCardinality (dictionary-encoded), library writer and reader"
$CLICKHOUSE_LOCAL -q "SELECT toLowCardinality('$u'::UUID2) AS lc SETTINGS output_format_arrow_use_native_writer = 0, output_format_arrow_low_cardinality_as_dictionary = 1, allow_suspicious_low_cardinality_types = 1 FORMAT Arrow" \
    | $CLICKHOUSE_LOCAL --input-format Arrow --input_format_arrow_use_native_reader 0 \
        -q "SELECT toTypeName(lc), toString(lc) FROM table"

echo "-- Arrow LowCardinality UUID (dictionary-encoded), library writer and reader"
$CLICKHOUSE_LOCAL -q "SELECT toLowCardinality('$u'::UUID) AS lc SETTINGS output_format_arrow_use_native_writer = 0, output_format_arrow_low_cardinality_as_dictionary = 1, allow_suspicious_low_cardinality_types = 1 FORMAT Arrow" \
    | $CLICKHOUSE_LOCAL --input-format Arrow --input_format_arrow_use_native_reader 0 \
        -q "SELECT toTypeName(lc), toString(lc) FROM table"
