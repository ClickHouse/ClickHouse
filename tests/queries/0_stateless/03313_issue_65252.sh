#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ver_kind() {
    "$@" --version 2>&1 |       \
        sed -nE 's/^ClickHouse[[:space:]]+([a-z]+)[[:space:]].*/\1/p'
}

echo "----CLICKHOUSE SHORTCUT CHECKS-----"
ver_kind "$CLICKHOUSE_BINARY"
ver_kind "$CLICKHOUSE_BINARY" --host sdfsadf
ver_kind "$CLICKHOUSE_BINARY" -h sdfsadf
ver_kind "$CLICKHOUSE_BINARY" --port 9000
ver_kind "$CLICKHOUSE_BINARY" --user qwr
ver_kind "$CLICKHOUSE_BINARY" -u qwr
ver_kind "$CLICKHOUSE_BINARY" --password secret

export CH_TMP="${CLICKHOUSE_BINARY%clickhouse}ch"
echo "----CH SHORTCUT CHECKS-----"
ver_kind "$CH_TMP"
ver_kind "$CH_TMP" --host sdfsadf
ver_kind "$CH_TMP" -h sdfsadf
ver_kind "$CH_TMP" --port 9000
ver_kind "$CH_TMP" --user qwr
ver_kind "$CH_TMP" -u qwr
ver_kind "$CH_TMP" --password secret