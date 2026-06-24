#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# --align-column-types aligns type tokens in CREATE TABLE column lists
echo "CREATE TABLE t (id UInt64, customer_booking_number String, amount Decimal(18, 2), has_flag Bool, created_at DateTime) ENGINE = MergeTree ORDER BY id;" | $CLICKHOUSE_FORMAT --align-column-types

# Mix of short and long names
echo "CREATE TABLE metrics (ts DateTime, count UInt64, ratio Float64, name String) ENGINE = MergeTree ORDER BY ts;" | $CLICKHOUSE_FORMAT --align-column-types

# Without the flag the output is unchanged (single space between name and type)
echo "CREATE TABLE t (id UInt64, customer_booking_number String) ENGINE = MergeTree ORDER BY id;" | $CLICKHOUSE_FORMAT
