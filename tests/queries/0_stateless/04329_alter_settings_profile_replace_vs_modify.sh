#!/usr/bin/env bash
# Regression test for the documented "replace vs. modify" behavior of ALTER SETTINGS PROFILE:
#   - a bare SETTINGS clause REPLACES the whole settings list and drops all inherited profiles;
#   - ADD/MODIFY/DROP change individual entries and leave the rest untouched.
# See docs/en/sql-reference/statements/alter/settings-profile.md

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Settings profiles are server-global, so use unique names to stay parallel-safe.
base="base_04329_${CLICKHOUSE_DATABASE}_$RANDOM"
p="p_04329_${CLICKHOUSE_DATABASE}_$RANDOM"

function show_create()
{
    $CLICKHOUSE_CLIENT -q "SHOW CREATE SETTINGS PROFILE ${p};" | sed -e "s/${p}/p/g" -e "s/${base}/base/g"
}

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS ${p}, ${base};"

$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE ${base} SETTINGS max_threads = 2;"

echo "--- populated profile: several settings + an inherited profile ---"
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE ${p} SETTINGS INHERIT ${base}, max_execution_time = 10, max_memory_usage = 100000000;"
show_create

echo "--- bare SETTINGS: replaces everything (other settings and the inherited profile are gone) ---"
$CLICKHOUSE_CLIENT -q "ALTER SETTINGS PROFILE ${p} SETTINGS readonly = 1;"
show_create

# Re-populate to test the incremental forms against a profile that already has content,
# including an inherited profile, so we can show the incremental forms preserve it.
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE OR REPLACE ${p} SETTINGS INHERIT ${base}, max_execution_time = 10, max_memory_usage = 100000000;"

echo "--- MODIFY SETTINGS: changes one entry, keeps the rest and the inherited profile ---"
$CLICKHOUSE_CLIENT -q "ALTER SETTINGS PROFILE ${p} MODIFY SETTINGS max_execution_time = 99;"
show_create

echo "--- ADD SETTINGS with constraints: keeps the rest ---"
$CLICKHOUSE_CLIENT -q "ALTER SETTINGS PROFILE ${p} ADD SETTINGS max_threads = 8 MIN 4 MAX 16 WRITABLE;"
show_create

echo "--- DROP SETTINGS: removes one, keeps the rest ---"
$CLICKHOUSE_CLIENT -q "ALTER SETTINGS PROFILE ${p} DROP SETTINGS max_memory_usage;"
show_create

echo "--- combined DROP + ADD in one statement ---"
$CLICKHOUSE_CLIENT -q "ALTER SETTINGS PROFILE ${p} DROP SETTINGS max_execution_time ADD SETTINGS readonly = 1;"
show_create

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE ${p}, ${base};"
