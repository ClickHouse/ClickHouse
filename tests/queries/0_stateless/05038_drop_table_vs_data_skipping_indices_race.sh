#!/usr/bin/env bash
# Tags: race

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The Map column matters: without a column that has dynamic subcolumns the size calculation keeps
# the parts lock for its whole duration and the interleaving below cannot happen.
for i in $(seq 1 30); do
    echo "CREATE TABLE t_$i (x UInt64, m Map(String, String), INDEX idx x TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY x;
          INSERT INTO t_$i SELECT number, map('k', toString(number)) FROM numbers(50);
          DROP TABLE t_$i;"
done | $CLICKHOUSE_CLIENT &

for _ in $(seq 1 60); do
    echo "SELECT * FROM system.data_skipping_indices WHERE database = '${CLICKHOUSE_DATABASE}' FORMAT Null;"
done | $CLICKHOUSE_CLIENT 2>/dev/null &

wait
