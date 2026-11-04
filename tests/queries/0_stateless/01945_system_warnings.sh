#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

build_type=`${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.build_options WHERE name='BUILD_TYPE'"`

if [[ $build_type == "Debug" ]]; then
    ${CLICKHOUSE_CLIENT} -q "SELECT message FROM system.warnings WHERE message LIKE '%built in debug mode%'"
else
    echo "Server was built in debug mode. It will work slowly."
fi

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.warnings WHERE message LIKE '%Obsolete setting%'"
${CLICKHOUSE_CLIENT} --multiple_joins_rewriter_version=42 -q "SELECT message FROM system.warnings WHERE message LIKE '%Obsolete setting%'"

# Avoid duplicated warnings
${CLICKHOUSE_CLIENT} -q "SELECT count() = countDistinct(message) FROM system.warnings"

# Avoid too many warnings, especially in CI
${CLICKHOUSE_CLIENT} -q "SELECT count() < 10 FROM system.warnings"

