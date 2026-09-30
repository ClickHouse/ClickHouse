#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

QUERIES=(
    "EXPLAIN AST BACKUP TABLE nonexistent_05296 TO S3('url', extra_credentials(headers('X-Auth' = 'SEKRIT_05296_BACKUP') = 'v'))"
    "EXPLAIN AST CREATE DATABASE db_05296 ENGINE = Backup('', S3('url', extra_credentials(headers('X-Auth' = 'SEKRIT_05296_DATABASE') = 'v')))"
)

for query in "${QUERIES[@]}"; do
    output=$($CLICKHOUSE_CLIENT --multiquery -q "SET format_display_secrets_in_show_and_select = 0; $query")
    [[ "$output" == *'[HIDDEN]'* && "$output" != *'SEKRIT_05296'* ]] && echo 1 || echo 0
done
