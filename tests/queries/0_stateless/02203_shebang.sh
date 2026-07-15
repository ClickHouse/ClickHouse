#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

script_path="${CUR_DIR}/$CLICKHOUSE_TEST_UNIQUE_NAME"
sed "s!/usr/bin/clickhouse-local!$(command -v ${CLICKHOUSE_LOCAL})!" "${CUR_DIR}/02203_shebang" > "$script_path"
chmod 755 "$script_path"
"$script_path"
rm -f "$script_path"
