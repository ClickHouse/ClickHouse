#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

# `finish_time` is stamped by background threads slightly after the mutation becomes
# observable as done through `system.mutations.is_done`, so poll for it.
function wait_for_finish_time()
{
    local table=$1
    for _ in {1..300}
    do
        if [[ $(${CLICKHOUSE_CLIENT} --query="SELECT countIf(finish_time = 0) FROM system.mutations WHERE database = currentDatabase() AND table = '$table'") -eq 0 ]]; then
            return
        fi
        sleep 0.3
    done

    echo "Timed out while waiting for finish_time on table $table"
    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.mutations WHERE database = currentDatabase() AND table = '$table' FORMAT Vertical"
}

${CLICKHOUSE_CLIENT} --query="
    CREATE TABLE mutations_finish_time (id UInt64, value UInt64)
    ENGINE = MergeTree ORDER BY id
    SETTINGS finished_mutations_to_keep = 100"

# A mutation on a table without parts has nothing to process and gets `finish_time` right away.
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations_finish_time UPDATE value = value + 1 WHERE id < 10 SETTINGS mutations_sync = 1"
wait_for_finish_time "mutations_finish_time"

${CLICKHOUSE_CLIENT} --query="
    SELECT 'empty', count(), countIf(is_done), countIf(finish_time >= create_time)
    FROM system.mutations
    WHERE database = currentDatabase() AND table = 'mutations_finish_time'"

${CLICKHOUSE_CLIENT} --query="INSERT INTO mutations_finish_time SELECT number, number FROM numbers(10)"

# Stop mutation execution to make the pending `finish_time = 0` state deterministic.
# `mutations_sync = 0` is explicit so that test environments enforcing synchronous
# mutations do not hang here while merges are stopped.
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES mutations_finish_time"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE mutations_finish_time UPDATE value = value + 1 WHERE 1 SETTINGS mutations_sync = 0"

${CLICKHOUSE_CLIENT} --query="
    SELECT 'unfinished', count(), countIf(finish_time = 0)
    FROM system.mutations
    WHERE database = currentDatabase() AND table = 'mutations_finish_time' AND is_done = 0"

${CLICKHOUSE_CLIENT} --query="SYSTEM START MERGES mutations_finish_time"
wait_for_all_mutations "mutations_finish_time"
wait_for_finish_time "mutations_finish_time"

${CLICKHOUSE_CLIENT} --query="
    SELECT 'finished', count(), countIf(is_done), countIf(finish_time >= create_time)
    FROM system.mutations
    WHERE database = currentDatabase() AND table = 'mutations_finish_time'"

# The two mutations update 0 and 10 rows.
${CLICKHOUSE_CLIENT} --query="SELECT 'data', sum(value - id) FROM mutations_finish_time"

${CLICKHOUSE_CLIENT} --query="DROP TABLE mutations_finish_time"
