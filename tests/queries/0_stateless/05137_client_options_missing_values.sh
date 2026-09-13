#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

for option in --format_csv_null_representation --max_threads; do
    for next_option in -q --query= --max_threads= --max-threads=; do
        if error=$($CLICKHOUSE_LOCAL "$option" "$next_option" --query "SELECT 1" 2>&1); then
            echo "Unexpectedly accepted $next_option as a value for $option"
            exit 1
        fi
        [[ "$error" == *"the required argument for option '$option' is missing"* ]]
        echo "Rejected $option before $next_option"
    done
done

for value in --query --multiline --multiquer --max-threads --format_csv_ --unknown_client_option --query=value --query=value=; do
    $CLICKHOUSE_LOCAL --format_csv_null_representation "$value" --query "SELECT getSetting('format_csv_null_representation')"
done

$CLICKHOUSE_LOCAL -q --multiline "SELECT 1"
$CLICKHOUSE_CLIENT -q --multiline "SELECT 2"
