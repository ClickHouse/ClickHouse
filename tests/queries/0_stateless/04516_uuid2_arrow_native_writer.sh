#!/usr/bin/env bash
# The default native Arrow writer (output_format_arrow_use_native_writer = 1) must emit a UUID2 column as a real
# Arrow UUID (fixed_size_binary(16) with the arrow.uuid extension), byte-identical to UUID for the same value,
# instead of silently falling back to raw Binary. It must also round-trip back to the correct textual value.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

u="61f0c404-5cb3-11e7-907b-a6006ad3dba0"

for format in Arrow ArrowStream
do
    echo "-- $format"
    # UUID2 must be written exactly like UUID (same schema and same canonical bytes).
    h2=$($CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID2 AS x FORMAT $format" | md5sum | cut -d' ' -f1)
    h1=$($CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID AS x FORMAT $format" | md5sum | cut -d' ' -f1)
    [ "$h2" = "$h1" ] && echo "bytes match UUID" || echo "bytes DIFFER from UUID"

    # Round-trip: the emitted UUID2 must decode back to the same textual value (as a UUID via the arrow.uuid extension).
    $CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID2 AS x FORMAT $format" \
        | $CLICKHOUSE_LOCAL --input-format "$format" -q "SELECT toTypeName(x), toString(x) FROM table"
done
