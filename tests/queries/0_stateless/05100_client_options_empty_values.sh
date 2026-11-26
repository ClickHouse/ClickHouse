#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

for option in --format_csv_null_representation= --format-csv-null-representation=; do
    $CLICKHOUSE_LOCAL "$option" --query "SELECT length(getSetting('format_csv_null_representation'))"
done

$CLICKHOUSE_LOCAL --query= --query "SELECT 1"
$CLICKHOUSE_LOCAL --format_csv_null_representation= --log_queries --query "SELECT length(getSetting('format_csv_null_representation')), toUInt8(getSetting('log_queries'))"
$CLICKHOUSE_CLIENT --allow_repeated_settings --format_csv_null_representation nonempty --format-csv-null-representation= --query "SELECT length(getSetting('format_csv_null_representation'))"

for option in --no-system-tables= --multiquery= --hilite= --format_csv_null_representatio= --unknown_client_option=; do
    if error=$($CLICKHOUSE_LOCAL "$option" --query "SELECT 1" 2>&1); then
        echo "Unexpectedly accepted $option"
        exit 1
    fi
    [[ "$error" == *'should follow immediately after the equal sign'* ]]
    echo "Rejected $option"
done
