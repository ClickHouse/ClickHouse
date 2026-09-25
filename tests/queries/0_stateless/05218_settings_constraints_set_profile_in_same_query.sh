#!/usr/bin/env bash
# A `SET` statement that changes `profile` installs a new constraint set halfway through itself, and the
# assignments and resets after that point must pass it, as they would in a separate statement.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONST_PROFILE="profile_const_$CLICKHOUSE_DATABASE"
READONLY_PROFILE="profile_readonly_$CLICKHOUSE_DATABASE"
MAX_PROFILE="profile_max_$CLICKHOUSE_DATABASE"
RELAXED_PROFILE="profile_relaxed_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -m -q "
DROP SETTINGS PROFILE IF EXISTS $CONST_PROFILE, $READONLY_PROFILE, $MAX_PROFILE, $RELAXED_PROFILE;
CREATE SETTINGS PROFILE $CONST_PROFILE SETTINGS max_execution_time = 10 CONST, SQL_tenant_id = 1 CONST;
CREATE SETTINGS PROFILE $READONLY_PROFILE SETTINGS readonly = 1;
CREATE SETTINGS PROFILE $MAX_PROFILE SETTINGS max_memory_usage MAX 1000000;
CREATE SETTINGS PROFILE $RELAXED_PROFILE SETTINGS max_memory_usage MAX 1099511627776;
"

# One session on purpose: a rejected statement must leave it untouched, which the statements after it prove.
$CLICKHOUSE_CLIENT -m -q "
SET profile = '$CONST_PROFILE', max_execution_time = 999; -- { serverError SETTING_CONSTRAINT_VIOLATION }
SET profile = '$CONST_PROFILE', max_execution_time = DEFAULT; -- { serverError SETTING_CONSTRAINT_VIOLATION }
SET profile = '$READONLY_PROFILE', max_memory_usage = 1099511627776; -- { serverError READONLY }
SET profile = '$MAX_PROFILE', max_memory_usage = 1099511627776; -- { serverError SETTING_CONSTRAINT_VIOLATION }
-- a later, looser profile does not lift the constraint in force at the assignment
SET profile = '$MAX_PROFILE', max_memory_usage = 5000000, profile = '$RELAXED_PROFILE'; -- { serverError SETTING_CONSTRAINT_VIOLATION }
-- none of the rejected statements applied its profile, which would have installed this setting
SELECT getSetting('SQL_tenant_id'); -- { serverError UNKNOWN_SETTING }
-- assigning the value the profile installs is a no-op and stays allowed
SET profile = '$CONST_PROFILE', max_execution_time = 10;
-- what is not constrained still applies, before and after the profile change
SET SQL_before = 7, profile = '$CONST_PROFILE', SQL_after = 8;
SELECT getSetting('SQL_before'), getSetting('SQL_after');
"

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE $CONST_PROFILE, $READONLY_PROFILE, $MAX_PROFILE, $RELAXED_PROFILE"
