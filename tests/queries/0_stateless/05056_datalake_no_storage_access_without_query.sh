#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ no-fasttest: the data lake engines are gated on build flags
# ^ no-msan: `delta-kernel-rs` is disabled under MSan

# Attaching a data lake table and listing it in `system.tables` must not read the object storage:
# the columns come from the table metadata and the totals are reported as unknown. When they did
# read it, a service whose bucket had gone away spent the S3 client's whole retry budget per table
# on every start, and every `system.tables` scan repeated one failing fetch per table, which kept
# the service in "Starting" for tens of minutes.
# https://github.com/ClickHouse/support-escalation/issues/8579

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An endpoint that serves no table, standing in for a bucket that was deleted or is unreachable.
GONE_S3="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}_gone"

# Both tables are listed with unknown totals, and not a single request is sent to the endpoint.
$CLICKHOUSE_LOCAL --multiquery "
CREATE DATABASE d ENGINE = Memory;
ATTACH TABLE d.t1 (x UInt64) ENGINE = IcebergS3('${GONE_S3}_iceberg/', 'clickhouse', 'clickhouse');
ATTACH TABLE d.t2 (x UInt64) ENGINE = DeltaLakeS3('${GONE_S3}_delta/', 'clickhouse', 'clickhouse');
SELECT count(), countIf(total_rows IS NULL), countIf(total_bytes IS NULL) FROM system.tables WHERE database = 'd';
SELECT sum(value) FROM system.events WHERE event LIKE 'S3%Request%';
"

# Reading a table still reports the failure. Local paths keep this offline and quick.
GONE_LOCAL="$CUR_DIR/data_delta_lake/${CLICKHOUSE_TEST_UNIQUE_NAME}_gone"
for engine in "IcebergLocal('${GONE_LOCAL}_iceberg')" "DeltaLakeLocal('${GONE_LOCAL}_delta')"; do
    $CLICKHOUSE_LOCAL --multiquery "
    CREATE DATABASE d ENGINE = Memory;
    ATTACH TABLE d.t (x UInt64) ENGINE = $engine;
    SELECT count() FROM d.t;
    " 2>&1 | grep -c -F 'DB::Exception'
done
