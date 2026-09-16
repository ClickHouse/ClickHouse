#!/usr/bin/env bash
# A `profile` change in a nested `SETTINGS` clause installs a new constraint set halfway through the
# clause, and the settings that follow it must not escape those constraints. Settings crossing into
# another execution context are clamped rather than rejected, so the nested clause loses to the
# constraint instead of throwing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PROFILE="profile_nested_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS $PROFILE"

$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE $PROFILE SETTINGS
    max_execution_time = 10 CONST,
    max_memory_usage MAX 1000000"

$CLICKHOUSE_CLIENT -q "SELECT * FROM (SELECT getSetting('max_execution_time') SETTINGS profile = '$PROFILE', max_execution_time = 999)"
$CLICKHOUSE_CLIENT -q "SELECT * FROM (SELECT getSetting('max_memory_usage') SETTINGS profile = '$PROFILE', max_memory_usage = 1099511627776)"
$CLICKHOUSE_CLIENT -q "WITH w AS (SELECT getSetting('max_execution_time') SETTINGS profile = '$PROFILE', max_execution_time = 999) SELECT * FROM w"

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE $PROFILE"
