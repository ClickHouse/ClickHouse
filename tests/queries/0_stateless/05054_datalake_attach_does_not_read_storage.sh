#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ no-fasttest: the DeltaLake engines are gated on build flags
# ^ no-msan: `delta-kernel-rs` is disabled under MSan

# Attaching a data lake table and listing it in `system.tables` must not read the table metadata from the
# storage. When they did, a table whose bucket had gone away made every server start pay a failing metadata
# fetch before the table was attached, and turned each `system.tables` scan into one failing fetch per row.
# Together that kept a service in "Starting" for tens of minutes.
# https://github.com/ClickHouse/support-escalation/issues/8579

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A path that does not exist, standing in for a bucket that was deleted or is unreachable.
MISSING="$CUR_DIR/data_delta_lake/${CLICKHOUSE_TEST_UNIQUE_NAME}_no_such_delta_table"

# ATTACH succeeds, and `system.tables` reports the table with unknown totals instead of failing per row.
$CLICKHOUSE_LOCAL --multiquery "
CREATE DATABASE d ENGINE = Memory;
ATTACH TABLE d.t (x UInt64) ENGINE = DeltaLakeLocal('$MISSING');
SELECT engine, total_rows IS NULL, total_bytes IS NULL FROM system.tables WHERE name = 't';
"

# Reading the table still reports the failure.
$CLICKHOUSE_LOCAL --multiquery "
CREATE DATABASE d ENGINE = Memory;
ATTACH TABLE d.t (x UInt64) ENGINE = DeltaLakeLocal('$MISSING');
SELECT count() FROM d.t;
" 2>&1 | grep -c -F 'DB::Exception'
