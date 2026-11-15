#!/usr/bin/env bash
# Tags: no-random-settings
# Dictionaries are updated using the server time.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e -o pipefail

# Wait when the dictionary will update the value for 13 on its own:
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

$CLICKHOUSE_CLIENT --query "SELECT '12 -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(12))"

$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_DATABASE}.table VALUES (13, 103, now())"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_DATABASE}.table VALUES (14, 104, now() - INTERVAL 1 DAY)"

if ! wait_for_dict_upate; then
    echo "Dictionary had not been reloaded" >&2
    exit 1
fi

$CLICKHOUSE_CLIENT --query "SELECT '13 -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(13))"

# By the way, the value for 14 is expected to not be updated at this moment,
# because the values were selected by the update field insert_time, and for 14 it was set as one day ago.
$CLICKHOUSE_CLIENT --query "SELECT '14 -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(14))"

# SYSTEM RELOAD DICTIONARY reloads it completely, regardless of the update field, so we will see new values, even for key 14.
$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD DICTIONARY '${CLICKHOUSE_DATABASE}.dict'"

$CLICKHOUSE_CLIENT --query "SELECT '12 (after reloading) -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(12))"
$CLICKHOUSE_CLIENT --query "SELECT '13 (after reloading) -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(13))"
$CLICKHOUSE_CLIENT --query "SELECT '14 (after reloading) -> ', dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'y', toUInt64(14))"
