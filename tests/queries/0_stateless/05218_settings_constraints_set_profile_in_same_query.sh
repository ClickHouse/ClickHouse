#!/usr/bin/env bash
# A `SET` statement that changes `profile` installs a new constraint set halfway through itself.
# Everything assigned or reset after that change must be checked against the new constraints, so
# that one statement cannot do what the same two statements in sequence are not allowed to do.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PROFILE="profile_const_$CLICKHOUSE_DATABASE"
PROFILE_READONLY="profile_readonly_$CLICKHOUSE_DATABASE"
PROFILE_MAX="profile_max_$CLICKHOUSE_DATABASE"
PROFILE_RELAXED="profile_relaxed_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS $PROFILE"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS $PROFILE_READONLY"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS $PROFILE_MAX"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS $PROFILE_RELAXED"

$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE $PROFILE SETTINGS
    max_execution_time = 10 CONST,
    SQL_tenant_id = 1 CONST"

echo '-- an explicit value after the profile change is checked against the new constraints'
$CLICKHOUSE_CLIENT -q "SET profile = '$PROFILE', max_execution_time = 999" 2>&1 | grep -o -m1 'SETTING_CONSTRAINT_VIOLATION'

echo '-- and so is a reset'
$CLICKHOUSE_CLIENT -q "SET profile = '$PROFILE', max_execution_time = DEFAULT" 2>&1 | grep -o -m1 'SETTING_CONSTRAINT_VIOLATION'
$CLICKHOUSE_CLIENT -q "SET profile = '$PROFILE', SQL_tenant_id = DEFAULT" 2>&1 | grep -o -m1 'SETTING_CONSTRAINT_VIOLATION'

# The profile is what would have installed `SQL_tenant_id`, so the setting being unknown proves the
# rejected statement above left the session alone instead of applying the profile and dropping its
# own tail. Both statements have to run in one session for that to mean anything.
echo '-- a rejected statement leaves the profile unapplied'
$CLICKHOUSE_CLIENT -m -q "
SET profile = '$PROFILE', max_execution_time = 999; -- { serverError SETTING_CONSTRAINT_VIOLATION }
SELECT getSetting('SQL_tenant_id');
" 2>&1 | grep -o -m1 'UNKNOWN_SETTING'

# An assignment placed *before* the profile change is not checked against the profile's constraints,
# because it takes effect before them. A custom setting on purpose: `clickhouse-client` keeps the
# settings a successful `SET` established and re-sends them with every later query, so a built-in
# setting assigned here and then overridden by the profile would be rejected while the next query's
# settings packet is received.
echo '-- an assignment before the profile change is not checked against the new constraints'
$CLICKHOUSE_CLIENT -m -q "
SET SQL_before_05218 = 7, profile = '$PROFILE';
SELECT getSetting('SQL_before_05218');
"

echo '-- the constraints the profile installed are in force afterwards'
$CLICKHOUSE_CLIENT -m -q "SET profile = '$PROFILE'; SET max_execution_time = 999" 2>&1 | grep -o -m1 'SETTING_CONSTRAINT_VIOLATION'
$CLICKHOUSE_CLIENT -m -q "SET profile = '$PROFILE'; SET max_execution_time = DEFAULT" 2>&1 | grep -o -m1 'SETTING_CONSTRAINT_VIOLATION'
$CLICKHOUSE_CLIENT -m -q "
SET profile = '$PROFILE';
SELECT getSetting('max_execution_time'), getSetting('SQL_tenant_id');
"

echo '-- assigning the value the profile installed is a no-op and stays allowed'
$CLICKHOUSE_CLIENT -q "SET profile = '$PROFILE', max_execution_time = 10"

echo '-- an unconstrained setting after the profile change still applies'
$CLICKHOUSE_CLIENT -m -q "
SET profile = '$PROFILE', SQL_unconstrained_05218 = 1234;
SELECT getSetting('SQL_unconstrained_05218');
"

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE $PROFILE"

$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE $PROFILE_READONLY SETTINGS
    max_memory_usage MAX 1000000,
    readonly = 1"

echo '-- a readonly profile rejects a value assigned after it'
$CLICKHOUSE_CLIENT -q "SET profile = '$PROFILE_READONLY', max_memory_usage = 1099511627776" 2>&1 | grep -o -m1 'READONLY'

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE $PROFILE_READONLY"

$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE $PROFILE_MAX SETTINGS max_memory_usage MAX 1000000"

echo '-- a MAX constraint rejects a larger value assigned after it'
$CLICKHOUSE_CLIENT -q "SET profile = '$PROFILE_MAX', max_memory_usage = 1099511627776" 2>&1 | grep -o -m1 'SETTING_CONSTRAINT_VIOLATION'

$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE $PROFILE_RELAXED SETTINGS max_memory_usage MAX 1099511627776"

echo '-- a later, looser profile change does not lift the constraint in force at the assignment'
$CLICKHOUSE_CLIENT -q "SET profile = '$PROFILE_MAX', max_memory_usage = 5000000, profile = '$PROFILE_RELAXED'" 2>&1 | grep -o -m1 'SETTING_CONSTRAINT_VIOLATION'

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE $PROFILE_MAX"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE $PROFILE_RELAXED"
