#!/usr/bin/env bash
# A `profile` change in a nested `SETTINGS` clause constrains the settings after it. Settings crossing into
# another execution context are clamped rather than rejected, so the nested value loses to the constraint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PROFILE="profile_nested_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -m -q "
DROP SETTINGS PROFILE IF EXISTS $PROFILE;
CREATE SETTINGS PROFILE $PROFILE SETTINGS max_execution_time = 10 CONST, max_memory_usage MAX 1000000;
SELECT * FROM (SELECT getSetting('max_execution_time') SETTINGS profile = '$PROFILE', max_execution_time = 999);
SELECT * FROM (SELECT getSetting('max_memory_usage') SETTINGS profile = '$PROFILE', max_memory_usage = 1099511627776);
WITH w AS (SELECT getSetting('max_execution_time') SETTINGS profile = '$PROFILE', max_execution_time = 999) SELECT * FROM w;
DROP SETTINGS PROFILE $PROFILE;
"
