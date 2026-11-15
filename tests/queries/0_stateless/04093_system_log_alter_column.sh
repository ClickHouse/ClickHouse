#!/usr/bin/env bash
# Verify that system log tables can be flushed after ALTER TABLE adds a column.
# This reproduces the bug where SystemLog::flushImpl would fail with
# "Invalid number of columns in chunk pushed to OutputPort" because the INSERT
# did not specify an explicit column list.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CONFIG_FILE="${CLICKHOUSE_TMP}/system_log_config.xml"
cat > "${CONFIG_FILE}" <<'EOF'
<clickhouse>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </query_log>
    <processors_profile_log>
        <database>system</database>
        <table>processors_profile_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </processors_profile_log>
</clickhouse>
EOF

$CLICKHOUSE_LOCAL --config-file "${CONFIG_FILE}" --multiquery "
    SET log_processors_profiles = 1;

    SELECT 1 FORMAT Null;
    SYSTEM FLUSH LOGS processors_profile_log;

    ALTER TABLE system.processors_profile_log ADD COLUMN IF NOT EXISTS extra_test_column UInt64 DEFAULT 0;

    SELECT 1 FORMAT Null;
    SYSTEM FLUSH LOGS processors_profile_log;

    SELECT count() > 0 FROM system.processors_profile_log;

    ALTER TABLE system.processors_profile_log DROP COLUMN IF EXISTS extra_test_column;

    SELECT 1 FORMAT Null;
    SYSTEM FLUSH LOGS query_log;

    ALTER TABLE system.query_log ADD COLUMN IF NOT EXISTS extra_test_column UInt64 DEFAULT 0;

    SELECT 1 FORMAT Null;
    SYSTEM FLUSH LOGS query_log;

    SELECT count() > 0 FROM system.query_log WHERE current_database = currentDatabase();

    ALTER TABLE system.query_log DROP COLUMN IF EXISTS extra_test_column;
"
