#!/usr/bin/env bash
# `system.table_settings`: a table's engine-specific settings, the engine's `system.engine_settings` rows with the
# table's own `SETTINGS` clause applied. Only settings a table states are checked for `MergeTree`, since
# `clickhouse-test` may add randomized `MergeTree` settings to the others. A shell test for the user that checks
# access: `EXECUTE AS` takes no query parameter, so a `.sql` test cannot name a user unique to its run.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -n -q "
SELECT '-- stated and altered settings, in the settings class form';
CREATE TABLE mt (a Int) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4096, min_bytes_for_wide_part = '10M';
ALTER TABLE mt MODIFY SETTING parts_to_throw_insert = 500;
SELECT name, value, changed FROM system.table_settings
WHERE database = currentDatabase() AND table = 'mt' AND name IN ('index_granularity', 'min_bytes_for_wide_part', 'parts_to_throw_insert')
ORDER BY name;

SELECT '-- a table that states nothing has exactly its engine rows';
CREATE TABLE mem (a Int) ENGINE = Memory;
SELECT count() FROM
(
    SELECT engine_name, name, value, default, changed, description, min, max, disallowed_values, readonly, type, is_obsolete, tier, inherited
    FROM system.table_settings WHERE database = currentDatabase() AND table = 'mem'
    EXCEPT SELECT engine_name, name, value, default, changed, description, min, max, disallowed_values, readonly, type, is_obsolete, tier, inherited FROM system.engine_settings WHERE engine_name = 'Memory'
);
SELECT count() FROM
(
    SELECT engine_name, name, value, default, changed, description, min, max, disallowed_values, readonly, type, is_obsolete, tier, inherited FROM system.engine_settings WHERE engine_name = 'Memory'
    EXCEPT SELECT engine_name, name, value, default, changed, description, min, max, disallowed_values, readonly, type, is_obsolete, tier, inherited
    FROM system.table_settings WHERE database = currentDatabase() AND table = 'mem'
);
CREATE TABLE mem_stated (a Int) ENGINE = Memory SETTINGS max_rows_to_keep = 100;
SELECT name, value FROM system.table_settings WHERE database = currentDatabase() AND table = 'mem_stated' AND changed;

SELECT '-- Distributed: a stated value, one stated through an alias, and the server fill-in';
CREATE TABLE dist_stated (a Int) ENGINE = Distributed(test_shard_localhost, currentDatabase(), mt) SETTINGS background_insert_batch = 1;
CREATE TABLE dist_alias (a Int) ENGINE = Distributed(test_shard_localhost, currentDatabase(), mt) SETTINGS monitor_batch_inserts = 1;
SELECT table, value, changed FROM system.table_settings
WHERE database = currentDatabase() AND table LIKE 'dist%' AND name = 'background_insert_batch'
ORDER BY table;
SELECT value = (SELECT value FROM system.engine_settings WHERE engine_name = 'Distributed' AND name = 'background_insert_sleep_time_ms')
FROM system.table_settings WHERE database = currentDatabase() AND table = 'dist_stated' AND name = 'background_insert_sleep_time_ms';

SELECT '-- a setting the engine never reads, such as a format setting of Set, is shown only where the definition states it';
CREATE TABLE set_plain (a Int) ENGINE = Set;
CREATE TABLE set_stated (a Int) ENGINE = Set SETTINGS format_csv_delimiter = ';';
SELECT table, name, value, changed FROM system.table_settings
WHERE database = currentDatabase() AND table LIKE 'set\\_%' ORDER BY table, name;

SELECT '-- a view has no engine settings';
CREATE VIEW v AS SELECT 1;
SELECT count() FROM system.table_settings WHERE database = currentDatabase() AND table = 'v';

SELECT '-- access: the SELECT grant, then SHOW TABLES per table';
DROP USER IF EXISTS ${user};
CREATE USER ${user};
"

$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT count() FROM system.table_settings" 2>&1 | grep -o -m1 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.table_settings TO ${user}"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT count() FROM system.table_settings WHERE database = '${CLICKHOUSE_DATABASE}'"
$CLICKHOUSE_CLIENT -q "GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.mem_stated TO ${user}"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT DISTINCT table FROM system.table_settings WHERE database = '${CLICKHOUSE_DATABASE}'"

echo "-- the condition cannot probe a database or a table the user cannot see"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_hidden"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT count() FROM system.table_settings WHERE NOT throwIf(database = '${CLICKHOUSE_DATABASE}_hidden', 'probed')"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT count() FROM system.table_settings WHERE NOT throwIf(table = 'mem', 'probed')"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${CLICKHOUSE_DATABASE}_hidden"

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
