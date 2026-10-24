#!/usr/bin/env bash
# Tags: no-parallel

# SYSTEM START RELOAD DICTIONARIES does not reload a dictionary unloaded after its reload was blocked.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e -o pipefail

# Reloads are stopped server-wide: never leave them stopped, even when the test fails.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM START RELOAD DICTIONARIES"' EXIT

$CLICKHOUSE_CLIENT <<EOF
CREATE TABLE ${CLICKHOUSE_DATABASE}.src (id UInt64, val Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${CLICKHOUSE_DATABASE}.src VALUES (1, 100);

CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.dict (id UInt64, val Int64 DEFAULT -1)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'src' DB '${CLICKHOUSE_DATABASE}'))
LAYOUT(FLAT())
LIFETIME(0);

SELECT dictGetInt64('${CLICKHOUSE_DATABASE}.dict', 'val', toUInt64(1));

SYSTEM STOP RELOAD DICTIONARIES;
SYSTEM RELOAD DICTIONARY ${CLICKHOUSE_DATABASE}.dict;
SYSTEM UNLOAD DICTIONARY ${CLICKHOUSE_DATABASE}.dict;
SYSTEM START RELOAD DICTIONARIES;

-- START requeues the blocked reloads before it returns, so a requeued one would already show as LOADING or LOADED.
SELECT status FROM system.dictionaries WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'dict';
EOF
