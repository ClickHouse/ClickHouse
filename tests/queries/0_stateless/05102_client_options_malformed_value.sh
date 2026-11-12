#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

for option in --format_csv_null_representatio= --format-csv-null-representatio= --hilite= --no-system-tables= --unknown_client_option= --=; do
    if error=$($CLICKHOUSE_LOCAL --query "$option" 2>&1); then
        echo "Unexpectedly accepted $option as a query"
        exit 1
    fi
    [[ "$error" == *'should follow immediately after the equal sign'* ]]
    echo "Rejected $option"
done

for option in --format_csv_null_representation= --format-csv-null-representation=; do
    if error=$($CLICKHOUSE_LOCAL --query "$option" --query "SELECT 1" 2>&1); then
        echo "Unexpectedly accepted $option as a query"
        exit 1
    fi
    [[ "$error" == *"the required argument for option '--query' is missing"* ]]
    echo "Rejected --query before $option"
done

if error=$($CLICKHOUSE_LOCAL --=value --query "SELECT 1" </dev/null 2>&1); then
    echo "Unexpectedly accepted --=value before --query"
    exit 1
fi
[[ "$error" == *"Unrecognized option '--=value'"* ]]
echo "Rejected --=value before --query"

if error=$($CLICKHOUSE_LOCAL --=value "SELECT 1" </dev/null 2>&1); then
    echo "Unexpectedly accepted --=value before positional query"
    exit 1
fi
[[ "$error" == *"Unrecognized option '--=value'"* ]]
echo "Rejected --=value before positional query"

for option in --format_csv_null_representation --max_threads --query; do
    if error=$($CLICKHOUSE_LOCAL "$option" --=value --query "SELECT getSetting('format_csv_null_representation')" </dev/null 2>&1); then
        echo "Unexpectedly accepted --=value after $option"
        exit 1
    fi
    [[ "$error" == *"Unrecognized option '--=value'"* ]]
    echo "Rejected --=value after $option"
done

$CLICKHOUSE_LOCAL --format_csv_null_representation --query=value= --query "SELECT getSetting('format_csv_null_representation')"
