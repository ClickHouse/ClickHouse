#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A LowCardinality dictionary of a floating-point type can hold entries whose bit patterns differ
# while their values compare equal. clickhouse-local keeps such a dictionary as parsed, which is
# why these cases are exercised here rather than through a server connection.
run() {
    $CLICKHOUSE_LOCAL --allow_suspicious_low_cardinality_types 1 --query "$1" < /dev/null
}

# Arming check: two entries with distinct bit patterns, one distinct value. Without it every
# assertion below would pass without reaching the code under test.
run "
    CREATE TABLE t (k LowCardinality(Float64), v UInt64) ENGINE = Memory;
    INSERT INTO t VALUES (-0.0, 10), (0.0, 20), (-0.0, 30), (0.0, 40);
    SELECT count() FROM (SELECT DISTINCT hex(reinterpretAsUInt64(k)) FROM t);
    SELECT count() FROM (SELECT DISTINCT toFloat64(k) FROM t);
    SELECT v FROM t ORDER BY k ASC, v ASC;
"

# Entries that compare differently keep their own order, while the value-equal pair is grouped.
run "
    CREATE TABLE t (k LowCardinality(Float64), v UInt64) ENGINE = Memory;
    INSERT INTO t VALUES (2.5, 1), (-0.0, 2), (1.5, 3), (0.0, 4), (2.5, 5), (-0.0, 6);
    SELECT k, v FROM t ORDER BY k ASC, v ASC;
"

run "
    CREATE TABLE t (k LowCardinality(BFloat16), v UInt64) ENGINE = Memory;
    INSERT INTO t VALUES (-0.0, 10), (0.0, 20), (-0.0, 30), (0.0, 40);
    SELECT v FROM t ORDER BY k ASC, v ASC;
"

run "
    CREATE TABLE t (k LowCardinality(Nullable(Float64)), v UInt64) ENGINE = Memory;
    INSERT INTO t VALUES (-0.0, 10), (NULL, 20), (0.0, 30), (NULL, 40), (-0.0, 50);
    SELECT v FROM t ORDER BY k ASC NULLS FIRST, v ASC;
"

# A non-float dictionary cannot hold value-equal entries, so its ordering must not change.
run "
    CREATE TABLE t (k LowCardinality(String), v UInt64) ENGINE = Memory;
    INSERT INTO t VALUES ('b', 10), ('a', 20), ('b', 30), ('a', 40);
    SELECT v FROM t ORDER BY k ASC, v ASC;
"

# A single sorting key takes the stable single-column sort the MergeTree writer applies to each
# inserted block, which persists the row order into the part. Rows of one equal range must stay in
# insertion order, so the result must match the one a plain Float64 key gives.
run "
    CREATE TABLE mt (k LowCardinality(Float64), v UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO mt VALUES (5.0, 10), (-0.0, 20), (0.0, 30), (5.0, 40), (0.0, 50), (-0.0, 60);
    SELECT groupArray(v) FROM (SELECT v FROM mt) SETTINGS max_threads = 1;
"
