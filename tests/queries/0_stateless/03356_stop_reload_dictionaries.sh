#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings
# Stateless test for SYSTEM STOP RELOAD DICTIONARIES

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e -o pipefail

function wait_for_dict_upate()
{
    for ((i = 0; i < 100; ++i)); do
        if [ "$(${CLICKHOUSE_CLIENT} --query "SELECT dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(13))")" != -1 ]; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

$CLICKHOUSE_CLIENT --query "DROP DICTIONARY IF EXISTS ${CLICKHOUSE_DATABASE}.dict"

$CLICKHOUSE_CLIENT <<EOF
CREATE TABLE ${CLICKHOUSE_DATABASE}.table(x Int64, y Int64, insert_time DateTime) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ${CLICKHOUSE_DATABASE}.table VALUES (12, 102, now());

CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.dict
(
  x Int64 DEFAULT -1,
  y Int64 DEFAULT -1,
  insert_time DateTime
)
PRIMARY KEY x
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table' DB '${CLICKHOUSE_DATABASE}' UPDATE_FIELD 'insert_time' UPDATE_LAG 60))
LAYOUT(FLAT())
LIFETIME(1);
EOF

# Stop dictionary reloading
$CLICKHOUSE_CLIENT --query "SYSTEM STOP RELOAD DICTIONARIES"

# Cannot load initial value for 12 while reload is blocked
set +e
OUT=$($CLICKHOUSE_CLIENT --query "SELECT '12 (initial load attempt 1) -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(12))" 2>&1)
set -e

if ! echo "$OUT" | grep -q "DB::Exception"; then
  echo "Expected DB::Exception"
fi

$CLICKHOUSE_CLIENT --query "SYSTEM START RELOAD DICTIONARIES"

# Can load initial value now that reload is unblocked.
$CLICKHOUSE_CLIENT --query "SELECT '12 (initial load attempt 2) -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(12))"

$CLICKHOUSE_CLIENT --query "SYSTEM STOP RELOAD DICTIONARIES"

# Insert new values.
$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_DATABASE}.table VALUES (13, 103, now())"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_DATABASE}.table VALUES (14, 104, now() - INTERVAL 1 DAY)"

# The value for 12 can be loaded but the other two cannot.
$CLICKHOUSE_CLIENT --query "SELECT '12 -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(12))"
$CLICKHOUSE_CLIENT --query "SELECT '13 -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(13))"
$CLICKHOUSE_CLIENT --query "SELECT '14 -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(14))"

# SYSTEM RELOAD DICTIONARIES has no effect.
$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD DICTIONARIES"

# SYSTEM RELOAD DICTIONARY has no effect.
$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD DICTIONARY '${CLICKHOUSE_DATABASE}.dict'"

# Still only the value for 12 can be read.
$CLICKHOUSE_CLIENT --query "SELECT '12 (2) -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(12))"
$CLICKHOUSE_CLIENT --query "SELECT '13 (2) -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(13))"
$CLICKHOUSE_CLIENT --query "SELECT '14 (2) -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(14))"

# Start reload again and wait for the dictionary to reload.
$CLICKHOUSE_CLIENT --query "SYSTEM START RELOAD DICTIONARIES"

if ! wait_for_dict_upate; then
    echo "Dictionary had not been reloaded" >&2
    exit 1
fi

# Values for 12 and 13 can be loaded. The value for 14 requires SYSTEM RELOAD DICTIONARIES.
$CLICKHOUSE_CLIENT --query "SELECT '12 (3) -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(12))"
$CLICKHOUSE_CLIENT --query "SELECT '13 (3) -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(13))"
$CLICKHOUSE_CLIENT --query "SELECT '14 (3) -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(14))"

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD DICTIONARIES"

# SYSTEM RELOADS DICTIONARIES reloads everything.
$CLICKHOUSE_CLIENT --query "SELECT '12 (4) -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(12))"
$CLICKHOUSE_CLIENT --query "SELECT '13 (4) -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(13))"
$CLICKHOUSE_CLIENT --query "SELECT '14 (4) -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(14))"

$CLICKHOUSE_CLIENT --query "DROP DICTIONARY ${CLICKHOUSE_DATABASE}.dict"
