#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_FORMAT --oneline --query "SELECT 1 INTO OUTFILE 'out.gz' TRUNCATE COMPRESSION 'GZ' LEVEL 9"
