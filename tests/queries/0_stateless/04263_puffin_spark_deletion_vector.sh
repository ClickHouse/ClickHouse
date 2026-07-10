#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PUFFIN="$CURDIR/data_puffin/04263_spark_deletion_vector.puffin"

echo "--- Spark-generated PuffinMetadata ---"
$CLICKHOUSE_LOCAL -q "
SELECT blob_type, snapshot_id, sequence_number, offset, length, compression_codec, mapKeys(properties), mapValues(properties)
FROM file('$PUFFIN', PuffinMetadata)
"

echo "--- Spark-generated Puffin: deleted rows ---"
$CLICKHOUSE_LOCAL -q "
SELECT deleted_rows
FROM file('$PUFFIN', Puffin)
"

echo "--- Spark-generated Puffin: ARRAY JOIN ---"
$CLICKHOUSE_LOCAL -q "
SELECT row_number
FROM file('$PUFFIN', Puffin)
ARRAY JOIN deleted_rows AS row_number
ORDER BY row_number
"
