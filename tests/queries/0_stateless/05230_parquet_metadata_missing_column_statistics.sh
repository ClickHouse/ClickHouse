#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A writer may omit column chunk statistics for some of the columns only. Every chunk must keep
# its own statistics, and the ones without must be reported as absent rather than shifted.
$CLICKHOUSE_LOCAL -q "
    SELECT columns.name, columns.have_statistics, columns.statistics
    FROM file('$CURDIR/data_parquet/datapage_v2.snappy.parquet', ParquetMetadata)
    ARRAY JOIN row_groups
    ARRAY JOIN row_groups.columns AS columns
    ORDER BY columns.name
"
