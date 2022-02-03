#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

sed -i.bak "s!/usr/bin/clickhouse-local!$(command -v ${CLICKHOUSE_LOCAL})!" 02203_shebang
./02203_shebang
