#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not supported in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `apply_string_filters_during_scan` with Parquet string pages in the `DELTA_BYTE_ARRAY` encoding
# (the file is written by pyarrow): the result must be the same with the setting enabled and disabled,
# and the values that do not match the filter are not materialized.

FILE="$CUR_DIR/data_parquet/string_filters_delta_byte_array.parquet"

for enable in 0 1; do
    $CLICKHOUSE_LOCAL -q "
    SELECT count(), sum(cityHash64(s)), sum(cityHash64(id)) FROM file('$FILE', Parquet) PREWHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = $enable;
    SELECT count(), sum(cityHash64(s)) FROM file('$FILE', Parquet) PREWHERE s LIKE 'lorem%' SETTINGS apply_string_filters_during_scan = $enable;
    SELECT count(), sum(cityHash64(s)) FROM file('$FILE', Parquet) PREWHERE s LIKE '%needle%' OR id = 1 SETTINGS apply_string_filters_during_scan = $enable;
    SELECT id, s FROM file('$FILE', Parquet) PREWHERE s LIKE '%needle%' ORDER BY id LIMIT 3 SETTINGS apply_string_filters_during_scan = $enable;
    "
done

echo 'the optimization is applied'
$CLICKHOUSE_LOCAL -q "
SELECT count() FROM file('$FILE', Parquet) PREWHERE s LIKE '%rare-substring%' SETTINGS apply_string_filters_during_scan = 1;
SELECT event, value > 0 FROM system.events
WHERE event IN ('StringValueFilterValuesChecked', 'StringValueFilterValuesReplaced', 'StringValueFilterBytesSkipped')
ORDER BY event;
"
