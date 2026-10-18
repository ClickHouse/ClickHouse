#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: requires S3
# Tag no-replicated-database: plain rewritable should not be shared between replicas

# Removing parts from a `plain_rewritable` disk must not copy their files.
# The parts truncated by `TRUNCATE` are removed either by the query itself or by the background cleanup,
# so wait until they are gone and look at `system.blob_storage_log` of a disk owned by this test
# instead of at the profile events of the query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function plain_rewritable_disk()
{
    local name=$1
    echo "disk(
        name = '${name}',
        type = s3_plain_rewritable,
        endpoint = 'http://localhost:11111/test/05315/${name}/',
        access_key_id = clickhouse,
        secret_access_key = clickhouse)"
}

function check()
{
    local table=$1
    local disk_definition=$2
    local disk_name=$3

    $CLICKHOUSE_CLIENT -m -q "
        DROP TABLE IF EXISTS ${table} SYNC;
        CREATE TABLE ${table} (a UInt64, s String) ENGINE = MergeTree ORDER BY a
        SETTINGS disk = ${disk_definition}, old_parts_lifetime = 0, merge_tree_clear_old_parts_interval_seconds = 1;
        SYSTEM STOP MERGES ${table};
        INSERT INTO ${table} SELECT number, toString(number) FROM numbers(100);
        INSERT INTO ${table} SELECT number, toString(number) FROM numbers(100, 100);
        INSERT INTO ${table} SELECT number, toString(number) FROM numbers(200, 100);
    "

    # Inserting a part and dropping a table unlink a few files outside of parts, so only the events after
    # `TRUNCATE` has started and before the table is dropped are checked.
    local truncate_time
    truncate_time=$($CLICKHOUSE_CLIENT -q "SELECT now64(6)")
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE ${table}"

    # Wait until the truncated parts are removed: `_state` makes `system.parts` show parts in every state.
    for _ in {1..600}
    do
        [[ $($CLICKHOUSE_CLIENT -q "
            SELECT count() FROM system.parts
            WHERE database = currentDatabase() AND table = '${table}' AND rows > 0 AND _state != ''") == 0 ]] && break
        sleep 0.1
    done

    $CLICKHOUSE_CLIENT -m -q "
        SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND rows > 0 AND _state != '';
        SYSTEM FLUSH LOGS blob_storage_log;
        SELECT countIf(event_type = 'Copy') AS copies, countIf(event_type = 'Delete') > 0 AS removed
        FROM system.blob_storage_log
        WHERE event_date >= yesterday() AND disk_name = '${disk_name}' AND event_time_microseconds >= '${truncate_time}';
        DROP TABLE ${table} SYNC;
    "
}

plain_disk="05315_plain_${CLICKHOUSE_DATABASE}"
check t_prr_plain "$(plain_rewritable_disk "$plain_disk")" "$plain_disk"

encrypted_inner_disk="05315_encrypted_inner_${CLICKHOUSE_DATABASE}"
check t_prr_encrypted "disk(
    type = encrypted,
    disk = disk(
        type = cache,
        max_size = '16Mi',
        path = '05315_cache_${CLICKHOUSE_DATABASE}/',
        disk = $(plain_rewritable_disk "$encrypted_inner_disk")),
    key = '1234567812345678')" "$encrypted_inner_disk"
