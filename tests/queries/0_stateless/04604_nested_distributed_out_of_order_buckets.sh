#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An intermediate node of a nested distributed query merges partially aggregated data and sends the result further.
# With `enable_producing_buckets_out_of_order_in_aggregation` it can send buckets out of order, and it has to report
# the delayed buckets to the next node, otherwise that node merges and outputs the same bucket twice,
# and the final result contains duplicated keys.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t SELECT number % 100000, number FROM numbers_mt(500000);

    -- The number of keys depends on the query id, so different shards produce different sets of buckets
    -- and these buckets become ready at different moments.
    CREATE VIEW t_leaf AS SELECT if(cityHash64(queryID()) % 2 = 0, k, k % 4) AS k, v FROM t;
    CREATE TABLE region AS remote('127.0.0.1,127.0.0.1', currentDatabase(), t_leaf);
"

# The query is racy: a single run reproduces the bug in about a half of the cases, so run it several times.
for _ in {1..8}; do
    $CLICKHOUSE_CLIENT -q "
        SELECT count() - uniqExact(k) AS duplicate_groups
        FROM (SELECT k, sum(v) AS s FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), region) GROUP BY k)
        SETTINGS prefer_localhost_replica = 0, group_by_two_level_threshold = 1, max_threads = 16,
            enable_producing_buckets_out_of_order_in_aggregation = 1;
    "
done
