#!/usr/bin/env bash
# Tags: no-fasttest
# ^ The Arrow library is not compiled into the fast-test build, so FORMAT Arrow would fail there.
# The default native Arrow writer (output_format_arrow_use_native_writer = 1) must emit a UUID2 column as a real
# Arrow UUID (fixed_size_binary(16) with the arrow.uuid extension) with the same canonical bytes as UUID for the
# same value, instead of silently falling back to raw Binary. The field metadata additionally carries the
# ClickHouse-specific discriminator (`ClickHouse:type` = `UUID2`), so a self-round-trip without an explicit
# schema restores the exact UUID2 type instead of degrading it to the historical UUID.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

u="61f0c404-5cb3-11e7-907b-a6006ad3dba0"

for format in Arrow ArrowStream
do
    echo "-- $format"
    # Round-trip without an explicit schema: both the type and the textual value must be preserved.
    $CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID2 AS x FORMAT $format" \
        | $CLICKHOUSE_LOCAL --input-format "$format" -q "SELECT toTypeName(x), toString(x) FROM table"

    # The emitted bytes are canonical, same as UUID for the same value: reading the UUID2-written data with an
    # explicit UUID schema must yield the same textual value (raw in-memory UUID2 bytes would decode differently).
    $CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID2 AS x FORMAT $format" \
        | $CLICKHOUSE_LOCAL --input-format "$format" --structure 'x UUID' -q "SELECT toTypeName(x), toString(x) FROM table"

    # A plain UUID column has no discriminator and must still read back as the historical UUID type.
    $CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID AS x FORMAT $format" \
        | $CLICKHOUSE_LOCAL --input-format "$format" -q "SELECT toTypeName(x), toString(x) FROM table"
done
