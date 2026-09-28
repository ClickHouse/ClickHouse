#!/usr/bin/env bash
# An `Alias` table whose target the user may not see. `SHOW TABLE SETTINGS` - which promises an error rather than an
# empty result for a table the user may not see - refuses it, rather than answering as if the alias had no settings.
# Once the target is visible too, the statement answers. An alias states no settings of its own, so
# `system.table_settings` has no rows for it whoever reads it, and its check on the target is only defensive.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
USER="${DB}_alias_reader"

# The flaky check runs a test many times against the same database, so it has to be re-runnable.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${DB}.alias_of_hidden"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${DB}.hidden_target"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.hidden_target (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4096"
$CLICKHOUSE_CLIENT --allow_experimental_alias_table_engine 1 -q "CREATE TABLE ${DB}.alias_of_hidden ENGINE = Alias('hidden_target')"

$CLICKHOUSE_CLIENT -q "CREATE USER ${USER} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.table_settings TO ${USER}"
$CLICKHOUSE_CLIENT -q "GRANT SHOW TABLES ON ${DB}.alias_of_hidden TO ${USER}"

echo "-- the target is hidden: the statement refuses the alias, as it refuses a table the user may not see"
$CLICKHOUSE_CLIENT --user="${USER}" -q "SHOW TABLE SETTINGS FROM ${DB}.alias_of_hidden" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo "-- once the target is visible, the statement answers"
$CLICKHOUSE_CLIENT -q "GRANT SHOW TABLES ON ${DB}.hidden_target TO ${USER}"
$CLICKHOUSE_CLIENT --user="${USER}" -q "SHOW TABLE SETTINGS FROM ${DB}.alias_of_hidden FORMAT Null; SELECT 'answered'"

$CLICKHOUSE_CLIENT -q "DROP USER ${USER}"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.alias_of_hidden"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.hidden_target"
