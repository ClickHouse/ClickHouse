#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS cancel_huge_mutation_subquery"
$CLICKHOUSE_CLIENT -n -q "
    CREATE TABLE cancel_huge_mutation_subquery (key Int, value String) Engine=MergeTree ORDER BY tuple() SETTINGS number_of_free_entries_in_pool_to_execute_mutation=0;
    INSERT INTO cancel_huge_mutation_subquery SELECT number, toString(number) FROM numbers(10000);"


$CLICKHOUSE_CLIENT --mutations_sync=2 -n -q "ALTER TABLE cancel_huge_mutation_subquery DELETE WHERE key IN (select number % 2 from numbers(100000000) where sleep(1) == 0)"   2>/dev/null &

# wait until mutation started
i=0
while [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE table = 'cancel_huge_mutation_subquery' and database='${CLICKHOUSE_DATABASE}' AND is_done = 0")" -ne 1 ]; do
    sleep 0.5
    i=$((i + 1))
    if [ $i -gt 100 ]; then
        echo "Mutation was not started in 5 seconds"
        exit 1
    fi
done

$CLICKHOUSE_CLIENT  --query "SYSTEM STOP MERGES cancel_huge_mutation_subquery"

# SYSTEM STOP MERGES prevents scheduling of new mutations, but if the background pool
# hasn't picked up the mutation task yet, the mutation entry stays with is_done=0 and
# no fail reason, causing mutations_sync=2 to wait forever. KILL MUTATION removes
# the entry from the mutation list and notifies the wait event.
$CLICKHOUSE_CLIENT --query "KILL MUTATION WHERE database='${CLICKHOUSE_DATABASE}' AND table='cancel_huge_mutation_subquery'" > /dev/null

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS cancel_huge_mutation_subquery"
