#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

MISSING="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_missing.orc"
PRESENT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_present.orc"

$CLICKHOUSE_CLIENT -q "SELECT toUInt64(1) AS id FORMAT ORC" > "$MISSING"
$CLICKHOUSE_CLIENT -q "SELECT toUInt64(1) AS id, toUInt32(7) AS counter FORMAT ORC" > "$PRESENT"

$CLICKHOUSE_CLIENT -q "
CREATE DICTIONARY d_missing (id UInt64, counter UInt32 DEFAULT 999) PRIMARY KEY id
SOURCE(FILE(path '$MISSING' format 'ORC')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0);
CREATE DICTIONARY d_present (id UInt64, counter UInt32 DEFAULT 999) PRIMARY KEY id
SOURCE(FILE(path '$PRESENT' format 'ORC')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0);

SELECT dictGetUInt32(currentDatabase() || '.d_missing', 'counter', toUInt64(1));
SELECT dictGetUInt32(currentDatabase() || '.d_present', 'counter', toUInt64(1));
"

rm -f "$MISSING" "$PRESENT"
