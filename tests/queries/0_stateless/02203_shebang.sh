#!/usr/bin/env bash

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

script_path="${CUR_DIR}/$CLICKHOUSE_TEST_UNIQUE_NAME"
cat <<EOF >"$script_path"
#!$(which clickhouse-local) --queries-file

SELECT 1;
EOF

chmod 755 "$script_path"
"$script_path"
rm -f "$script_path"

rm -rf "${home_dir}"
