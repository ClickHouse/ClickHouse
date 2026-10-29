#!/usr/bin/env bash
# Tags: no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Without --tmp or --path, clickhouse-local locks its default data directory in the home, so parallel tests
# that run it through the bare binary must each get a home of their own.
home_dir="${CLICKHOUSE_TMP}/home_${CLICKHOUSE_DATABASE}"
rm -rf "$home_dir"
mkdir -p "$home_dir"

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_without_extension
echo "SELECT 'Hello from a file'" > ${FILE}

# Queries can be read from a file.
HOME="$home_dir" XDG_DATA_HOME="" ${CLICKHOUSE_BINARY} --queries-file ${FILE}

# Or from stdin.
HOME="$home_dir" XDG_DATA_HOME="" ${CLICKHOUSE_BINARY} < ${FILE}

# Also the positional argument can be interpreted as a file.
HOME="$home_dir" XDG_DATA_HOME="" ${CLICKHOUSE_BINARY} ${FILE}

${CLICKHOUSE_LOCAL} --queries-file ${FILE}
${CLICKHOUSE_LOCAL} < ${FILE}
${CLICKHOUSE_LOCAL} ${FILE}

${CLICKHOUSE_CLIENT} --queries-file ${FILE}
${CLICKHOUSE_CLIENT} < ${FILE}
${CLICKHOUSE_CLIENT} ${FILE}

# Check that positional arguments work in any place
echo "Select name, changed, value FROM system.settings where name = 'max_local_read_bandwidth'" > ${FILE}
HOME="$home_dir" XDG_DATA_HOME="" ${CLICKHOUSE_BINARY} ${FILE} --max-local-read-bandwidth 100
HOME="$home_dir" XDG_DATA_HOME="" ${CLICKHOUSE_BINARY} --max-local-read-bandwidth 200 ${FILE}

rm ${FILE}
rm -rf "$home_dir"
