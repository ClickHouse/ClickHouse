#!/usr/bin/env bash
# Tags: no-fasttest

# https://github.com/ClickHouse/ClickHouse/issues/109189
# Writing LowCardinality(Time) to the Arrow output format with
# output_format_arrow_low_cardinality_as_dictionary=1 used to hit a
# LOGICAL_ERROR "Cannot fill arrow array time32 with LowCardinality(Time) data"
# because the arrow value type time32 was missing from the LowCardinality
# dictionary dispatch.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.arrow

# 100000 rows, 3 distinct values: exercises the arrow dictionary builder (dedup).
$CLICKHOUSE_LOCAL -q "
    SET allow_experimental_time_time64_type = 1;
    SET allow_suspicious_low_cardinality_types = 1;
    SET output_format_arrow_low_cardinality_as_dictionary = 1;
    SELECT CAST(['01:02:03','12:34:56','23:59:59'][number % 3 + 1]::Time AS LowCardinality(Time)) AS t
    FROM numbers(100000)
    INTO OUTFILE '$DATA_FILE' TRUNCATE FORMAT Arrow;
"

# Verify the produced Arrow file: the column must be a dictionary of time32[s]
# and decode back to exactly the three time values with the expected counts.
python3 -c "
import pyarrow as pa
import pyarrow.feather as feather

table = feather.read_table('$DATA_FILE')
field = table.schema.field('t')
# ClickHouse LowCardinality -> Arrow Dictionary; value type is time32[s].
assert pa.types.is_dictionary(field.type), 'expected a dictionary type, got ' + str(field.type)
assert pa.types.is_time32(field.type.value_type), 'expected time32 value type, got ' + str(field.type.value_type)

decoded = table.column('t').combine_chunks().dictionary_decode().to_pylist()
from collections import Counter
counts = Counter(v.strftime('%H:%M:%S') for v in decoded)
for v in sorted(counts):
    print(v, counts[v])
"

rm -f "${DATA_FILE:?}"
