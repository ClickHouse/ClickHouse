#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

script_path="${CUR_DIR}/$CLICKHOUSE_TEST_UNIQUE_NAME"
cat <<EOF >"$script_path"
#!$(which clickhouse-local) --queries-file

SELECT 1;
EOF

chmod 755 "$script_path"
"$script_path"
rm -f "$script_path"
