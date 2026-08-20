#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `clickhouse local` attaches the system tables on the first access to the `system` database. This works both for
# the ephemeral `system` database that it creates on its own and for a persistent one that it loads from `--path`,
# as a running server leaves behind: the tables that come from disk stay visible, and the system table storages are
# added on top of them on demand.

test_dir="${CLICKHOUSE_TMP}/05023_${CLICKHOUSE_DATABASE}"

rm -rf "${test_dir}"
mkdir -p "${test_dir}/metadata/system" "${test_dir}/data/system/persisted"

# A persistent `system` database with a table in it, as a running server leaves behind.
echo "ATTACH DATABASE system ENGINE=Ordinary" > "${test_dir}/metadata/system.sql"
echo "ATTACH TABLE system.persisted (x UInt8) ENGINE = MergeTree ORDER BY x;" > "${test_dir}/metadata/system/persisted.sql"

local_query()
{
    ${CLICKHOUSE_LOCAL} --path "${test_dir}" --query "$1"
}

# A `FROM`-less query does not need any of the deferred tables.
local_query "SELECT 1"

# The persisted table is writable and readable, and it is not lost when the system tables are attached.
local_query "INSERT INTO system.persisted VALUES (42)"
local_query "SELECT x FROM system.persisted"
local_query "SELECT count() FROM system.persisted, system.one"

# The deferred system tables resolve on demand, one by one and through the table list.
local_query "SELECT count() FROM (SELECT number FROM system.numbers LIMIT 3)"
local_query "SELECT count() > 100 FROM system.tables WHERE database = 'system'"
local_query "SELECT count() FROM system.tables WHERE database = 'system' AND name IN ('one', 'numbers', 'persisted')"
local_query "SHOW TABLES FROM system LIKE 'persisted'"

# A name that a deferred system table occupies is not free, even though it is not attached yet.
local_query "CREATE TABLE system.numbers (x UInt8) ENGINE = Memory" 2>&1 | grep -c "already exists"

# `information_schema` is deferred in the same way.
local_query "SELECT count() > 0 FROM information_schema.tables WHERE table_schema = 'system'"

rm -rf "${test_dir}"
