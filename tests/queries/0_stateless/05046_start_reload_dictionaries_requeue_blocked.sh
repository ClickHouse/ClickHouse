#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings

# SYSTEM START RELOAD DICTIONARIES must requeue a config-driven or explicit reload that was
# blocked while stopped, for a dictionary with LIFETIME(0) (i.e. one with no periodic update
# cycle to pick it up on its own later).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e -o pipefail

function wait_for_dict_update()
{
    for ((i = 0; i < 100; ++i)); do
        if [ "$(${CLICKHOUSE_CLIENT} --query "SELECT dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'val', toUInt64(2))")" == "200" ]; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

$CLICKHOUSE_CLIENT --query "DROP DICTIONARY IF EXISTS ${CLICKHOUSE_DATABASE}.dict"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.src"

$CLICKHOUSE_CLIENT <<EOF
CREATE TABLE ${CLICKHOUSE_DATABASE}.src(id Int64, val Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ${CLICKHOUSE_DATABASE}.src VALUES (1, 100);

CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.dict
(
  id Int64 DEFAULT -1,
  val Int64 DEFAULT -1
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'src' DB '${CLICKHOUSE_DATABASE}'))
LAYOUT(FLAT())
LIFETIME(0);
EOF

# Trigger the initial (lazy) load.
$CLICKHOUSE_CLIENT --query "SELECT dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'val', toUInt64(1))"

$CLICKHOUSE_CLIENT --query "SYSTEM STOP RELOAD DICTIONARIES"

# A row that only exists once reload is blocked.
$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_DATABASE}.src VALUES (2, 200)"

# Blocked: has no effect, and does not throw (unlike the plural SYSTEM RELOAD DICTIONARIES).
$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD DICTIONARY '${CLICKHOUSE_DATABASE}.dict'"

# The dictionary is still loaded with the old data: id=1 is still visible, id=2 is not yet.
$CLICKHOUSE_CLIENT --query "SELECT dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'val', toUInt64(1))"
$CLICKHOUSE_CLIENT --query "SELECT dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'val', toUInt64(2))"

# LIFETIME(0) means this dictionary has no periodic update cycle of its own to fall back on:
# only requeuing the blocked reload attempt on START can make it pick up id=2, with no further
# SYSTEM RELOAD DICTIONARY or config change needed.
$CLICKHOUSE_CLIENT --query "SYSTEM START RELOAD DICTIONARIES"

if ! wait_for_dict_update; then
    echo "Dictionary had not been reloaded" >&2
    exit 1
fi

$CLICKHOUSE_CLIENT --query "DROP DICTIONARY ${CLICKHOUSE_DATABASE}.dict"
$CLICKHOUSE_CLIENT --query "DROP TABLE ${CLICKHOUSE_DATABASE}.src"
