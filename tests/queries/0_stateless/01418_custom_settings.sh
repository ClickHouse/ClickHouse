#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PROFILE1="s1_${CLICKHOUSE_TEST_UNIQUE_NAME}"
PROFILE2="s2_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -n -q "
DROP SETTINGS PROFILE IF EXISTS ${PROFILE1}, ${PROFILE2};

SELECT '--- assigning ---';
SET custom_a = 5;
SET custom_b = -177;
SET custom_c = 98.11;
SET custom_d = 'abc def';
SELECT getSetting('custom_a') as v, toTypeName(v);
SELECT getSetting('custom_b') as v, toTypeName(v);
SELECT getSetting('custom_c') as v, toTypeName(v);
SELECT getSetting('custom_d') as v, toTypeName(v);
SELECT name, value FROM system.settings WHERE name LIKE 'custom_%' ORDER BY name;

SELECT '--- modifying ---';
SET custom_a = 'changed';
SET custom_b = NULL;
SET custom_c = 50000;
SET custom_d = 1.11;
SELECT getSetting('custom_a') as v, toTypeName(v);
SELECT getSetting('custom_b') as v, toTypeName(v);
SELECT getSetting('custom_c') as v, toTypeName(v);
SELECT getSetting('custom_d') as v, toTypeName(v);
SELECT name, value FROM system.settings WHERE name LIKE 'custom_%' ORDER BY name;

SELECT '--- undefined setting ---';
"

# Expected error: custom_e is not yet defined
${CLICKHOUSE_CLIENT} -q "SELECT getSetting('custom_e') as v, toTypeName(v)" 2>&1 | grep -m1 -o 'UNKNOWN_SETTING'

${CLICKHOUSE_CLIENT} -n -q "
SET custom_e = 404;
SELECT getSetting('custom_e') as v, toTypeName(v);

SELECT '--- wrong prefix ---';
"

# Expected error: invalid_custom is not a valid prefix
${CLICKHOUSE_CLIENT} -q "SET invalid_custom = 8" 2>&1 | grep -m1 -o 'UNKNOWN_SETTING'

# Re-SET custom_e since it was in a previous session.
# Run getSetting('custom_f') in the same session to verify query-level SETTINGS don't leak.
ERR_FILE=$(mktemp)
${CLICKHOUSE_CLIENT} -n -q "
SET custom_e = 404;

SELECT '--- using query context ---';
SELECT getSetting('custom_e') as v, toTypeName(v) SETTINGS custom_e = -0.333;
SELECT name, value FROM system.settings WHERE name = 'custom_e' SETTINGS custom_e = -0.333;
SELECT getSetting('custom_e') as v, toTypeName(v);
SELECT name, value FROM system.settings WHERE name = 'custom_e';

SELECT getSetting('custom_f') as v, toTypeName(v) SETTINGS custom_f = 'word';
SELECT name, value FROM system.settings WHERE name = 'custom_f' SETTINGS custom_f = 'word';
SELECT getSetting('custom_f') as v, toTypeName(v);
" 2>"$ERR_FILE"

# Expected error: custom_f was used via query-level SETTINGS but not SET in session
grep -m1 -o 'UNKNOWN_SETTING' "$ERR_FILE"
rm -f "$ERR_FILE"

${CLICKHOUSE_CLIENT} -n -q "
SELECT COUNT() FROM system.settings WHERE name = 'custom_f';

SELECT '--- compound identifier ---';
SET custom_compound.identifier.v1 = 'test';
SELECT getSetting('custom_compound.identifier.v1') as v, toTypeName(v);
SELECT name, value FROM system.settings WHERE name = 'custom_compound.identifier.v1';

CREATE SETTINGS PROFILE ${PROFILE1} SETTINGS custom_compound.identifier.v2 = 100;
"

${CLICKHOUSE_CLIENT} -q "SHOW CREATE SETTINGS PROFILE ${PROFILE1}" | sed "s/${PROFILE1}/s1_01418/g"

${CLICKHOUSE_CLIENT} -n -q "
DROP SETTINGS PROFILE ${PROFILE1};

SELECT '--- null type ---';
SELECT getSetting('custom_null') as v, toTypeName(v) SETTINGS custom_null = NULL;
SELECT name, value FROM system.settings WHERE name = 'custom_null' SETTINGS custom_null = NULL;

SET custom_null = NULL;
SELECT getSetting('custom_null') as v, toTypeName(v);
SELECT name, value FROM system.settings WHERE name = 'custom_null';

CREATE SETTINGS PROFILE ${PROFILE2} SETTINGS custom_null = NULL;
"

${CLICKHOUSE_CLIENT} -q "SHOW CREATE SETTINGS PROFILE ${PROFILE2}" | sed "s/${PROFILE2}/s2_01418/g"

${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE ${PROFILE2}"
