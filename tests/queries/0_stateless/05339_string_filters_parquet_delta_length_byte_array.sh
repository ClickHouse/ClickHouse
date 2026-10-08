#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `apply_string_filters_during_scan` on `DELTA_LENGTH_BYTE_ARRAY` string pages, which ClickHouse does not write.
DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
trap 'rm -f "$DATA_FILE"' EXIT

python3 -c "
import pyarrow as pa
import pyarrow.parquet as pq

n = 30000
s = ['value with needle %d' % i if i % 10 == 0 else ('' if i % 7 == 0 else 'ordinary value %d' % i) for i in range(n)]
v = [None if i % 13 == 0 else ('nullable needle %d' % i if i % 10 == 0 else 'plain value %d' % i) for i in range(n)]
schema = pa.schema([pa.field('id', pa.uint32(), nullable=False), pa.field('s', pa.string(), nullable=False), pa.field('n', pa.string())])
table = pa.table({'id': list(range(n)), 's': s, 'n': v}, schema=schema)
pq.write_table(table, '$DATA_FILE', use_dictionary=False, row_group_size=10000,
               column_encoding={'s': 'DELTA_LENGTH_BYTE_ARRAY', 'n': 'DELTA_LENGTH_BYTE_ARRAY'})
"

for enable in 0 1; do
    $CLICKHOUSE_LOCAL -q "
        SELECT count(), sum(cityHash64(s)) FROM file('$DATA_FILE', Parquet) PREWHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = $enable;
        SELECT count(), sum(cityHash64(s)) FROM file('$DATA_FILE', Parquet) PREWHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = $enable, input_format_parquet_max_block_size = 1000;
        SELECT count(), sum(cityHash64(s)) FROM file('$DATA_FILE', Parquet) PREWHERE id % 3 = 0 AND s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = $enable;
        SELECT count(), sum(cityHash64(n)) FROM file('$DATA_FILE', Parquet) PREWHERE n LIKE '%needle%' SETTINGS apply_string_filters_during_scan = $enable;
        SELECT id, s, n FROM file('$DATA_FILE', Parquet) PREWHERE s LIKE '%needle%' ORDER BY id LIMIT 3 SETTINGS apply_string_filters_during_scan = $enable;
    "
done

echo 'the filter is applied'
for condition in "s LIKE '%needle%'" "id % 3 = 0 AND s LIKE '%needle%'"; do
    $CLICKHOUSE_LOCAL --print-profile-events --profile-events-delay-ms=-1 \
        -q "SELECT count() FROM file('$DATA_FILE', Parquet) PREWHERE $condition SETTINGS apply_string_filters_during_scan = 1" 2>&1 >/dev/null \
        | grep -oE 'StringValueFilterValues(Checked|Replaced): [0-9]+'
done
