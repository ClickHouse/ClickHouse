#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o errexit
set -o pipefail

# Exercise `SHOW CREATE TABLE` for every attached system table while discarding
# the large documentation comments from the output.
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.tables WHERE database = 'system' ORDER BY name FORMAT TSVRaw" |
    while IFS= read -r table; do
        ${CLICKHOUSE_CLIENT} --param_table "$table" --query "SHOW CREATE TABLE system.{table:Identifier} FORMAT Null"
    done

echo OK
