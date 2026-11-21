#!/usr/bin/env bash
# Tags: no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Without `--path` or `--tmp`, `clickhouse local` locks a persistent data directory inside the home directory.
# Point HOME (and XDG_DATA_HOME) at a private directory, so that concurrent tests do not fight over that lock.
home_dir="${CLICKHOUSE_TMP}/home-${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${home_dir}"
mkdir -p "${home_dir}"
export HOME="${home_dir}"
export XDG_DATA_HOME=""

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_without_extension
echo "SELECT 'Hello from a file'" > ${FILE}

# Queries can be read from a file.
${CLICKHOUSE_BINARY} --queries-file ${FILE}

# Or from stdin.
${CLICKHOUSE_BINARY} < ${FILE}

# Also the positional argument can be interpreted as a file.
${CLICKHOUSE_BINARY} ${FILE}

${CLICKHOUSE_LOCAL} --queries-file ${FILE}
${CLICKHOUSE_LOCAL} < ${FILE}
${CLICKHOUSE_LOCAL} ${FILE}

${CLICKHOUSE_CLIENT} --queries-file ${FILE}
${CLICKHOUSE_CLIENT} < ${FILE}
${CLICKHOUSE_CLIENT} ${FILE}

# Check that positional arguments work in any place
echo "Select name, changed, value FROM system.settings where name = 'max_local_read_bandwidth'" > ${FILE}
${CLICKHOUSE_BINARY} ${FILE} --max-local-read-bandwidth 100
${CLICKHOUSE_BINARY} --max-local-read-bandwidth 200 ${FILE}

rm ${FILE}

rm -rf "${home_dir}"
