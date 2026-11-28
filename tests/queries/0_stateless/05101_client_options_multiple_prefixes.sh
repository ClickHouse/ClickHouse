#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

$CLICKHOUSE_LOCAL --hili=0 --format TSV --format_csv_delimite '|' --format_csv_null_representatio NONE --query "SELECT getSetting('format_csv_delimiter'), getSetting('format_csv_null_representation')"
