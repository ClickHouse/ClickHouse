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

# Wait until the refresh is inside that nested pipeline. Fail hard on timeout: a drop that never
# meets a blocked refresh returns quickly for the wrong reason, and the test would then match the
# reference without exercising the cancellation path it covers.
i=0
while [ "$($CLICKHOUSE_CLIENT -q "
        select count() from system.processes
        where current_database = currentDatabase() and query like 'INSERT INTO%sleepEachRow%' and elapsed > 2")" -ne 1 ]; do
    sleep 0.3
    i=$((i + 1))
    if [ "$i" -gt 200 ]; then
        echo "Refresh did not reach the planning stage in time" >&2
        exit 1
    fi
done

# The drop must cancel the refresh, not wait for the blocked planning to finish.
if timeout 10 $CLICKHOUSE_CLIENT -q "drop table rmv"; then
    echo "dropped"
else
    echo "drop did not finish"
fi

$CLICKHOUSE_CLIENT -q "drop table src"
