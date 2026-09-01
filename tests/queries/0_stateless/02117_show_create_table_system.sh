#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o errexit
set -o pipefail

# Exercise `SHOW CREATE TABLE` for every attached system table while discarding
# the large documentation comments from the output. Run the statements through
# one client connection so sanitizer builds do not spend minutes starting a
# separate process for every table.
${CLICKHOUSE_CLIENT} --query "SELECT format('SHOW CREATE TABLE system.{} FORMAT Null;', name) FROM system.tables WHERE database = 'system' ORDER BY name FORMAT TSVRaw" |
    ${CLICKHOUSE_CLIENT} --multiquery

echo OK
