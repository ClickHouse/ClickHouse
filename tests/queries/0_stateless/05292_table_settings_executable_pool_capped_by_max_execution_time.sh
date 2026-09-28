#!/usr/bin/env bash
# `ExecutablePool` caps the `max_command_execution_time` it fills in by the server's `max_execution_time`, so a new
# table takes 5 seconds here rather than 10, and `system.engine_settings` has to say so rather than report the
# compiled-in default. `Executable` fills in nothing, and keeps the default. `clickhouse-local`, since the value is read
# from the settings of the global context, which a session's `SET` does not reach.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --max_execution_time 5 -q "
SELECT 'engine', engine, value, \`default\`, changed, source FROM system.engine_settings
WHERE engine IN ('Executable', 'ExecutablePool') AND name = 'max_command_execution_time' ORDER BY engine;
CREATE TABLE pool (x UInt64) ENGINE = ExecutablePool('nonexistent.sh', 'TabSeparated');
SELECT 'table', value, \`default\`, changed, source FROM system.table_settings
WHERE table = 'pool' AND name = 'max_command_execution_time';"
