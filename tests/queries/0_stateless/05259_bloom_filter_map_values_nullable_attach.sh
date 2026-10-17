#!/usr/bin/env bash

# A bloom_filter index over mapValues(m) of LowCardinality(Nullable(String)) values has the type Array(Nullable(String)),
# which the index refuses. CREATE rejects such an index, and a full ATTACH still loads it so that it can be dropped.
# An INSERT into such a table fails like its merges do, instead of writing parts that can never be merged.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A full ATTACH into an Atomic database needs a UUID, and concurrent runs of this test must not share one.
uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT --send_logs_level fatal -q "
    ATTACH TABLE t_legacy UUID '$uuid' (k UInt32, m Map(String, LowCardinality(Nullable(String))), INDEX ix mapValues(m) TYPE bloom_filter)
    ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT --send_logs_level fatal -q "INSERT INTO t_legacy VALUES (1, map('a', 'x', 'b', NULL))" 2>&1 \
    | grep -m1 -o 'Unexpected type Array(Nullable(String)) of bloom filter index'

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_legacy DROP INDEX ix"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_legacy VALUES (1, map('a', 'x', 'b', NULL))"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_legacy"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_legacy"
