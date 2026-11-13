#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Arrow and Parquet support are not compiled into the fast-test build.

# Arrow and Parquet are self-describing formats, and both flavors of ClickHouse UUID map to the same
# UUID metadata there (the `arrow.uuid` extension / the parquet UUID logical type). A self-round-trip
# of a `UUID2` column without an explicit schema must restore the exact `UUID2` type - not silently
# degrade it to the historical `UUID`, which would lose the correct ordering semantics. The writers
# record a ClickHouse-specific discriminator (Arrow field metadata `ClickHouse:type` = `UUID2`; the
# parquet footer key-value `ClickHouse:uuid2_leaf_columns`) and schema inference prefers it, while an
# explicit schema hint always wins. Plain `UUID` columns carry no discriminator and keep reading back
# as `UUID`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

u="61f0c404-5cb3-11e7-907b-a6006ad3dba0"

echo "-- Arrow, all writer/reader combinations"
for w in 1 0
do
    for r in 1 0
    do
        echo -n "native_writer=$w native_reader=$r: "
        $CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID2 AS x, '$u'::UUID AS y SETTINGS output_format_arrow_use_native_writer = $w FORMAT Arrow" \
            | $CLICKHOUSE_LOCAL --input-format Arrow --input_format_arrow_use_native_reader "$r" \
                -q "SELECT toTypeName(x), toTypeName(y), toString(x), toString(y) FROM table"
    done
done

echo "-- ArrowStream"
$CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID2 AS x FORMAT ArrowStream" \
    | $CLICKHOUSE_LOCAL --input-format ArrowStream -q "SELECT toTypeName(x), toString(x) FROM table"

echo "-- Arrow, nested and Nullable"
$CLICKHOUSE_LOCAL -q "SELECT ['$u'::UUID2] AS arr, '$u'::Nullable(UUID2) AS n FORMAT Arrow" \
    | $CLICKHOUSE_LOCAL --input-format Arrow -q "SELECT toTypeName(arr), toTypeName(n), toString(arr[1]), toString(n) FROM table"

echo "-- Arrow, explicit schema hint wins"
$CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID2 AS x FORMAT Arrow" \
    | $CLICKHOUSE_LOCAL --input-format Arrow --structure 'x UUID' -q "SELECT toTypeName(x), toString(x) FROM table"
$CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID AS x FORMAT Arrow" \
    | $CLICKHOUSE_LOCAL --input-format Arrow --structure 'x UUID2' -q "SELECT toTypeName(x), toString(x) FROM table"

echo "-- Parquet, scalar and mixed"
$CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID2 AS x, '$u'::UUID AS y FORMAT Parquet" \
    | $CLICKHOUSE_LOCAL --input-format Parquet -q "SELECT toTypeName(x), toTypeName(y), toString(x), toString(y) FROM table"

echo "-- Parquet, nested (per-leaf discrimination) and Nullable"
$CLICKHOUSE_LOCAL -q "SELECT ['$u'::UUID2] AS arr, map('$u'::UUID2, '$u'::UUID) AS m, tuple('$u'::UUID2, '$u'::UUID)::Tuple(a UUID2, b UUID) AS t, '$u'::Nullable(UUID2) AS n FORMAT Parquet" \
    | $CLICKHOUSE_LOCAL --input-format Parquet -q "SELECT toTypeName(arr), toTypeName(m), toTypeName(t), toTypeName(n), toString(arr[1]), toString(n) FROM table"

echo "-- Parquet, explicit schema hint wins"
$CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID2 AS x FORMAT Parquet" \
    | $CLICKHOUSE_LOCAL --input-format Parquet --structure 'x UUID' -q "SELECT toTypeName(x), toString(x) FROM table"
$CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID AS x FORMAT Parquet" \
    | $CLICKHOUSE_LOCAL --input-format Parquet --structure 'x UUID2' -q "SELECT toTypeName(x), toString(x) FROM table"

echo "-- Parquet, filter pushdown on an inferred UUID2 column"
$CLICKHOUSE_LOCAL -q "
    SELECT number::UInt128::UUID2 AS x FROM numbers(10000)
    SETTINGS output_format_parquet_row_group_size = 1000 FORMAT Parquet" > "${CLICKHOUSE_TMP}/04633_uuid2.parquet"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('${CLICKHOUSE_TMP}/04633_uuid2.parquet') WHERE x = 5555::UInt128::UUID2"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('${CLICKHOUSE_TMP}/04633_uuid2.parquet') WHERE x > 9000::UInt128::UUID2"
rm -f "${CLICKHOUSE_TMP}/04633_uuid2.parquet"
