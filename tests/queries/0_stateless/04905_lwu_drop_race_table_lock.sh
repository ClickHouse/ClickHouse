#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
# Tag no-replicated-database: the test needs its own Memory database, which a Replicated database
#   run cannot host, and DROP must reach the synchronous exclusive-lock path.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A Memory database has no UUID, so DROP takes the table's exclusive lock and removes the data
# inline instead of deferring it to the background cleanup an Atomic database uses.
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_1}" < /dev/null
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLICKHOUSE_DATABASE_1} ENGINE = Memory" < /dev/null

function setup_table()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE_1}.t SYNC;

        CREATE TABLE ${CLICKHOUSE_DATABASE_1}.t (id UInt64, c2 String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04905_$1/', '1')
        ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

        INSERT INTO ${CLICKHOUSE_DATABASE_1}.t SELECT number, 'a' FROM numbers(2);

        ALTER TABLE ${CLICKHOUSE_DATABASE_1}.t
            MODIFY SETTING sleep_before_commit_local_part_in_replicated_table_ms = 10000;
    " < /dev/null
}

# The sink sleeps right before it commits the patch part, so a DROP issued a few seconds into the
# update lands in the window between the part rename and the commit.
function race_with_drop()
{
    ${CLICKHOUSE_CLIENT} "$@" < /dev/null 2>/dev/null &
    local updater=$!

    sleep 3
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE_1}.t SYNC" < /dev/null 2>/dev/null || true

    wait $updater 2>/dev/null || true
}

setup_table update
race_with_drop --enable_lightweight_update 1 \
    --query "UPDATE ${CLICKHOUSE_DATABASE_1}.t SET c2 = 'xx' WHERE id = 1"
${CLICKHOUSE_CLIENT} --query "SELECT 'update survived'" < /dev/null

setup_table alter
race_with_drop --enable_lightweight_update 1 --alter_update_mode 'lightweight_force' \
    --query "ALTER TABLE ${CLICKHOUSE_DATABASE_1}.t UPDATE c2 = 'xx' WHERE id = 1"
${CLICKHOUSE_CLIENT} --query "SELECT 'alter survived'" < /dev/null

# An Alias table resolves a different storage, so the update has to hold the target's lock as well.
setup_table alias
${CLICKHOUSE_CLIENT} --allow_experimental_alias_table_engine 1 --query "
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE_1}.a SYNC;
    CREATE TABLE ${CLICKHOUSE_DATABASE_1}.a ENGINE = Alias('${CLICKHOUSE_DATABASE_1}', 't');
" < /dev/null
race_with_drop --allow_experimental_alias_table_engine 1 --enable_lightweight_update 1 \
    --query "UPDATE ${CLICKHOUSE_DATABASE_1}.a SET c2 = 'xx' WHERE id = 1"
${CLICKHOUSE_CLIENT} --query "SELECT 'alias survived'" < /dev/null

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLICKHOUSE_DATABASE_1}" < /dev/null
