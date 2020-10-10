#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# The following command will execute:
#     CREATE TABLE table (key UInt32) ENGINE = File(TSV, stdin);
#     INSERT INTO `table` SELECT key FROM input('key UInt32') FORMAT TSV
${CLICKHOUSE_LOCAL} -S 'key UInt32' -q "INSERT INTO \`table\` SELECT key FROM input('key UInt32') FORMAT TSV" < /dev/null
