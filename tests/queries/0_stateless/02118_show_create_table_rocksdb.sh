#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so rocksdb engine is not enabled by default

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The documentation comment of a system table is large and is checked elsewhere, so it is cut off here.
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE system.rocksdb" | sed 's/\\nCOMMENT .*$//'
