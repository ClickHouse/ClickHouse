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

# `RENAME` cannot take over a deferred system table name either, and the rejected source table survives.
local_query "CREATE TABLE system.renamed (x UInt8) ENGINE = MergeTree ORDER BY x; RENAME TABLE system.renamed TO system.numbers" 2>&1 | grep -c "already exists"
local_query "SELECT count() FROM system.tables WHERE database = 'system' AND name = 'renamed'"

# The same for the ephemeral `system` database of a `--path`-less invocation.
${CLICKHOUSE_LOCAL} --query "CREATE TABLE system.renamed (x UInt8) ENGINE = Memory; RENAME TABLE system.renamed TO system.numbers" 2>&1 | grep -c "already exists"

# `information_schema` is deferred in the same way.
local_query "SELECT count() > 0 FROM information_schema.tables WHERE table_schema = 'system'"

rm -rf "${test_dir}"

# A stored table cannot take over the name of a system table that is not attached yet: an old `--path` may
# contain a table whose name a later version gave to a system table, and reading it instead of the system table
# would be worse than the collision an eager attachment reports at startup - `system.settings` and the like are
# readable without any grants.
shadow_dir="${CLICKHOUSE_TMP}/05023_shadow_${CLICKHOUSE_DATABASE}"

rm -rf "${shadow_dir}"
mkdir -p "${shadow_dir}/metadata/system" "${shadow_dir}/data/system/settings"

echo "ATTACH DATABASE system ENGINE=Ordinary" > "${shadow_dir}/metadata/system.sql"
echo "ATTACH TABLE system.settings (x UInt8) ENGINE = MergeTree ORDER BY x;" > "${shadow_dir}/metadata/system/settings.sql"

# Reading the name populates the database, and the collision is reported instead of the stored table.
${CLICKHOUSE_LOCAL} --path "${shadow_dir}" --query "SELECT * FROM system.settings" 2>&1 | grep -c "already exists"
# The same through the table list, and for a query that does not name the table at all.
${CLICKHOUSE_LOCAL} --path "${shadow_dir}" --query "SHOW TABLES FROM system" 2>&1 | grep -c "already exists"
# A query that needs none of the deferred tables still works.
${CLICKHOUSE_LOCAL} --path "${shadow_dir}" --query "SELECT 1"

rm -rf "${shadow_dir}"

# The same coverage for an `Atomic` persistent `system` database - the layout a running server actually leaves
# behind (the `Ordinary` layout above only remains from installations that predate the automatic conversion).
# `RENAME` and `DETACH` go through `DatabaseAtomic` here rather than `DatabaseOnDisk`. The database is staged
# under a different name and renamed on disk: the database metadata file stores a placeholder, not the name.
atomic_dir="${CLICKHOUSE_TMP}/05023_atomic_${CLICKHOUSE_DATABASE}"

rm -rf "${atomic_dir}"
${CLICKHOUSE_LOCAL} --path "${atomic_dir}" --query "CREATE DATABASE sysstage ENGINE = Atomic; CREATE TABLE sysstage.persisted (x UInt8) ENGINE = MergeTree ORDER BY x; INSERT INTO sysstage.persisted VALUES (42)"
mv "${atomic_dir}/metadata/sysstage.sql" "${atomic_dir}/metadata/system.sql"
mv "${atomic_dir}/metadata/sysstage" "${atomic_dir}/metadata/system"

atomic_query()
{
    ${CLICKHOUSE_LOCAL} --path "${atomic_dir}" --query "$1"
}

# The loaded database is `Atomic`, and the persisted table survives next to the deferred system tables.
atomic_query "SELECT name, engine FROM system.databases WHERE name = 'system'"
atomic_query "SELECT x FROM system.persisted"
atomic_query "SELECT count() FROM system.persisted, system.one"

# The reserved names hold for `CREATE` and `RENAME` through `DatabaseAtomic` as well, and the rejected source
# table survives.
atomic_query "CREATE TABLE system.numbers (x UInt8) ENGINE = Memory" 2>&1 | grep -c "already exists"
atomic_query "CREATE TABLE system.renamed (x UInt8) ENGINE = MergeTree ORDER BY x; RENAME TABLE system.renamed TO system.numbers" 2>&1 | grep -c "already exists"
atomic_query "SELECT count() FROM system.tables WHERE database = 'system' AND name = 'renamed'"

rm -rf "${atomic_dir}"

# A stored table cannot shadow a deferred system table in an `Atomic` persistent `system` database either.
atomic_shadow_dir="${CLICKHOUSE_TMP}/05023_atomic_shadow_${CLICKHOUSE_DATABASE}"

rm -rf "${atomic_shadow_dir}"
${CLICKHOUSE_LOCAL} --path "${atomic_shadow_dir}" --query "CREATE DATABASE sysstage ENGINE = Atomic; CREATE TABLE sysstage.settings (x UInt8) ENGINE = MergeTree ORDER BY x"
mv "${atomic_shadow_dir}/metadata/sysstage.sql" "${atomic_shadow_dir}/metadata/system.sql"
mv "${atomic_shadow_dir}/metadata/sysstage" "${atomic_shadow_dir}/metadata/system"

${CLICKHOUSE_LOCAL} --path "${atomic_shadow_dir}" --query "SELECT * FROM system.settings" 2>&1 | grep -c "already exists"
${CLICKHOUSE_LOCAL} --path "${atomic_shadow_dir}" --query "SHOW TABLES FROM system" 2>&1 | grep -c "already exists"
${CLICKHOUSE_LOCAL} --path "${atomic_shadow_dir}" --query "SELECT 1"

rm -rf "${atomic_shadow_dir}"
