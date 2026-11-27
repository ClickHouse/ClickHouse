#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings

# SYSTEM START RELOAD DICTIONARIES requeues the reloads blocked while stopped in one batch, like
# SYSTEM RELOAD DICTIONARIES: a dictionary that reads another one is rebuilt from that one's new version.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e -o pipefail

# Reloads are stopped server-wide: never leave them stopped, even when the test fails.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM START RELOAD DICTIONARIES"' EXIT

$CLICKHOUSE_CLIENT <<EOF
CREATE TABLE ${CLICKHOUSE_DATABASE}.src_a (id UInt64, val Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${CLICKHOUSE_DATABASE}.src_a VALUES (1, 100);

-- Slow, so dict_b's reload reaches dict_a while dict_a is still reloading.
CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.dict_a (id UInt64, val Int64 DEFAULT -1)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default'
    QUERY 'SELECT id, val FROM ${CLICKHOUSE_DATABASE}.src_a WHERE sleepEachRow(1) = 0'))
LAYOUT(FLAT())
LIFETIME(0);

CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.dict_b (id UInt64, val Int64 DEFAULT -1)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default'
    QUERY 'SELECT id, val FROM dictionary(\'${CLICKHOUSE_DATABASE}.dict_a\')'))
LAYOUT(FLAT())
LIFETIME(0);
EOF

# Loads dict_b, and dict_a through it.
$CLICKHOUSE_CLIENT --query "SELECT dictGetInt64('${CLICKHOUSE_DATABASE}.dict_b', 'val', toUInt64(1))"

$CLICKHOUSE_CLIENT --query "SYSTEM STOP RELOAD DICTIONARIES"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_DATABASE}.src_a VALUES (2, 200)"
$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD DICTIONARY '${CLICKHOUSE_DATABASE}.dict_a'"
$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD DICTIONARY '${CLICKHOUSE_DATABASE}.dict_b'"
$CLICKHOUSE_CLIENT --query "SYSTEM START RELOAD DICTIONARIES"

# Wait until both requeued reloads are done.
for ((i = 0; i < 100; ++i)); do
    if [ "$($CLICKHOUSE_CLIENT --query "SELECT dictGetInt64('${CLICKHOUSE_DATABASE}.dict_a', 'val', toUInt64(2))")" == "200" ] \
        && [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.dictionaries WHERE database = '${CLICKHOUSE_DATABASE}' AND status = 'LOADED'")" == "2" ]; then
        break
    fi
    sleep 0.5
done

$CLICKHOUSE_CLIENT --query "SELECT dictGetInt64('${CLICKHOUSE_DATABASE}.dict_a', 'val', toUInt64(2))"
$CLICKHOUSE_CLIENT --query "SELECT dictGetInt64('${CLICKHOUSE_DATABASE}.dict_b', 'val', toUInt64(2))"
