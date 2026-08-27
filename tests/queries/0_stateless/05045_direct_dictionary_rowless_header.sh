#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
CREATE DICTIONARY dict (word String, counter UInt32 DEFAULT 999)
PRIMARY KEY word
SOURCE(HTTP(url '${CLICKHOUSE_URL}&query=SELECT+%27Hello%27%2C1+FORMAT+CSV' format 'CSV'))
LAYOUT(DIRECT());

SELECT * FROM dict ORDER BY word;
SELECT * FROM merge(currentDatabase(), '^dict\$') ORDER BY word;
"
