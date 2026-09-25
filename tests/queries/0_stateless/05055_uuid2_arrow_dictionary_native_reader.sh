#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Arrow support is not compiled into the fast-test build.

# A dictionary-encoded (`LowCardinality`) column written by the Apache Arrow library writer cannot carry
# the `arrow.uuid` extension keys: the registered extension type rejects dictionary storage, so the writer
# marks such a column with the ClickHouse-specific discriminator (`ClickHouse:type`) alone. The native
# Arrow IPC reader must treat that discriminator as authoritative, otherwise the column reads back as
# `LowCardinality(FixedString(16))` instead of `LowCardinality(UUID2)`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

u="61f0c404-5cb3-11e7-907b-a6006ad3dba0"

for t in UUID2 UUID
do
    echo -n "library writer, dictionary-encoded LowCardinality($t), native reader: "
    $CLICKHOUSE_LOCAL -q "SELECT toLowCardinality('$u'::$t) AS lc SETTINGS output_format_arrow_use_native_writer = 0, output_format_arrow_low_cardinality_as_dictionary = 1, allow_suspicious_low_cardinality_types = 1 FORMAT Arrow" \
        | $CLICKHOUSE_LOCAL --input-format Arrow --input_format_arrow_use_native_reader 1 \
            -q "SELECT toTypeName(lc), toString(lc) FROM table"
done
