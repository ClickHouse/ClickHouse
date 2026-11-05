#!/usr/bin/env bash
# Tags: no-parallel

# A load queued while reloads are stopped, whose thread only starts after SYSTEM START RELOAD DICTIONARIES,
# is not skipped as blocked.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e -o pipefail

FAILPOINT=external_loader_pause_before_loading

# Reloads are stopped server-wide: never leave them stopped, even when the test fails.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"; $CLICKHOUSE_CLIENT --query "SYSTEM START RELOAD DICTIONARIES"' EXIT

$CLICKHOUSE_CLIENT <<EOF
CREATE TABLE ${CLICKHOUSE_DATABASE}.src (id UInt64, val Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${CLICKHOUSE_DATABASE}.src VALUES (1, 100);

-- Not lazy, so ATTACH queues its load without waiting for it.
CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.dict (id UInt64, val Int64)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'src' DB '${CLICKHOUSE_DATABASE}'))
LAYOUT(FLAT())
LIFETIME(0)
SETTINGS(dictionary_lazy_load = 0);

DETACH DICTIONARY ${CLICKHOUSE_DATABASE}.dict;
SYSTEM STOP RELOAD DICTIONARIES;
SYSTEM ENABLE FAILPOINT ${FAILPOINT};
ATTACH DICTIONARY ${CLICKHOUSE_DATABASE}.dict;
SYSTEM WAIT FAILPOINT ${FAILPOINT} PAUSE;
SYSTEM START RELOAD DICTIONARIES;
SYSTEM NOTIFY FAILPOINT ${FAILPOINT};
EOF

for ((i = 0; i < 100; ++i)); do
    if [ "$($CLICKHOUSE_CLIENT --query "SELECT status FROM system.dictionaries WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'dict'")" == "LOADED" ]; then
        break
    fi
    sleep 0.5
done

$CLICKHOUSE_CLIENT --query "SELECT status FROM system.dictionaries WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'dict'"
