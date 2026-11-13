#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Arrow support is not compiled into the fast-test build.

# A dictionary-encoded (`LowCardinality`) field cannot carry the `arrow.uuid` extension keys: Arrow rejects the UUID
# extension over dictionary storage, so external readers refuse such a schema. The native Arrow writer must mark a
# dictionary-encoded `UUID` / `UUID2` field with the ClickHouse-specific discriminator (`ClickHouse:type`) alone,
# and ClickHouse must still read the exact type back.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

u="61f0c404-5cb3-11e7-907b-a6006ad3dba0"

for format in Arrow ArrowStream
do
    for t in UUID2 UUID
    do
        for as_dictionary in 1 0
        do
            query="SELECT toLowCardinality('$u'::$t) AS lc SETTINGS output_format_arrow_low_cardinality_as_dictionary = $as_dictionary, allow_suspicious_low_cardinality_types = 1 FORMAT $format"
            echo -n "$format, LowCardinality($t), as dictionary = $as_dictionary, has arrow.uuid extension: "
            $CLICKHOUSE_LOCAL -q "$query" | grep -a -q 'arrow.uuid' && echo 1 || echo 0
            echo -n "$format, LowCardinality($t), as dictionary = $as_dictionary, read back: "
            $CLICKHOUSE_LOCAL -q "$query" | $CLICKHOUSE_LOCAL --input-format $format -q "SELECT toTypeName(lc), toString(lc) FROM table"
        done
    done
done
