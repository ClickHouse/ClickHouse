#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A `direct` dictionary reads through the source's own pipeline, and `merge` wraps every child in
# `materialize`, which rejects a header that carries rows.
$CLICKHOUSE_CLIENT -q "
    CREATE DICTIONARY dict (word String, counter UInt32)
    PRIMARY KEY word
    SOURCE(HTTP(url '${CLICKHOUSE_URL}&query=SELECT+%27Hello%27,1+FORMAT+CSV' format 'CSV'))
    LAYOUT(DIRECT())"

$CLICKHOUSE_CLIENT -q "SELECT * FROM merge(currentDatabase(), '^dict\$')"
