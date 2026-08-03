#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    create table src (k UInt64) engine MergeTree order by k as select number from numbers(10);"

# `k IN (subquery)` over the primary key materializes the set during query planning
# (ReadFromMergeTree::buildIndexes -> KeyCondition -> FutureSetFromSubquery::buildOrderedSetInplace),
# so the refresh blocks in a nested pipeline that it does not own an executor for yet.
$CLICKHOUSE_CLIENT -q "
    create materialized view rmv refresh every 1 second (k UInt64) engine MergeTree order by k as
        select k from src where k in (select number from numbers(30) where sleepEachRow(1) = 0)
        settings max_block_size = 1;"

# Wait until the refresh is inside that nested pipeline.
for _ in {1..300}; do
    started=$($CLICKHOUSE_CLIENT -q "
        select count() from system.processes
        where current_database = currentDatabase() and query like 'INSERT INTO%sleepEachRow%' and elapsed > 2")
    if [ "$started" = "1" ]; then
        break
    fi
    sleep 0.2
done

# The drop must cancel the refresh, not wait for the blocked planning to finish.
if timeout 10 $CLICKHOUSE_CLIENT -q "drop table rmv"; then
    echo "dropped"
else
    echo "drop did not finish"
fi

$CLICKHOUSE_CLIENT -q "drop table src"
