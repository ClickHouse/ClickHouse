#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

$CLICKHOUSE_LOCAL --max_threads 3 --query "SELECT getSetting('max_threads')"
$CLICKHOUSE_LOCAL --max-threads=4 -q "SELECT getSetting('max_threads')"
$CLICKHOUSE_LOCAL --hilite=0 --format TSV --query "SELECT 5"
$CLICKHOUSE_LOCAL --format_csv_delimite '|' --query "SELECT getSetting('format_csv_delimiter')"
$CLICKHOUSE_LOCAL --enable_analyzer 1 --query "SELECT getSetting('allow_experimental_analyzer')"
$CLICKHOUSE_LOCAL --query "SELECT 6" "SELECT 7"
$CLICKHOUSE_LOCAL --format_csv_null_representation='' --query "SELECT NULL FORMAT CSV"
$CLICKHOUSE_LOCAL --query="SELECT 'value'" --format=TSV
$CLICKHOUSE_LOCAL --query "SELECT getSetting('log_queries')" --log_queries
$CLICKHOUSE_LOCAL --format_csv_delimiter=- --query "SELECT getSetting('format_csv_delimiter')"
$CLICKHOUSE_LOCAL --format_csv_delimiter - --query "SELECT getSetting('format_csv_delimiter')"
$CLICKHOUSE_CLIENT --allow_repeated_settings --max_threads 6 --max_threads=7 --query "SELECT getSetting('max_threads')"
$CLICKHOUSE_CLIENT --allow_repeated_settings --query "SELECT 8" --max_threads=2 --query "SELECT 9" --max_threads=3
$CLICKHOUSE_CLIENT --allow_merge_tree_settings --index_granularity 1024 --query "SELECT 10"
$CLICKHOUSE_LOCAL -q"SELECT 11"
$CLICKHOUSE_LOCAL "SELECT 12"
$CLICKHOUSE_LOCAL -nq"SELECT 13"
$CLICKHOUSE_LOCAL --format_csv_null_representation -NULL --query "SELECT NULL FORMAT CSV"
$CLICKHOUSE_LOCAL --query "SELECT 14" --
$CLICKHOUSE_LOCAL --log-level=fatal --query "SELECT 15" -- -logger.level=information
$CLICKHOUSE_LOCAL --log-level=fatal --query "SELECT 16" -- -logger.level information
$CLICKHOUSE_LOCAL --log-level=fatal --query "SELECT 17" -- --logger.level=information
$CLICKHOUSE_LOCAL --log_queries --query "SELECT getSetting('log_queries')"

if error=$($CLICKHOUSE_LOCAL --max_threads=1 --max_threads=2 --query "SELECT 1" 2>&1); then
    echo 'Unexpectedly accepted repeated settings'
    exit 1
fi
[[ "$error" == *"cannot be specified more than once"* ]]
echo 'Repeated settings rejected'

if error=$($CLICKHOUSE_LOCAL --format_csv_ value --query "SELECT 1" 2>&1); then
    echo 'Unexpectedly accepted an ambiguous option'
    exit 1
fi
[[ "$error" == *"ambiguous"* ]]
echo 'Ambiguous option rejected'

if error=$($CLICKHOUSE_LOCAL --query 2>&1); then
    echo 'Unexpectedly accepted a missing option value'
    exit 1
fi
[[ "$error" == *"required argument"* ]]
echo 'Missing option value rejected'

if error=$($CLICKHOUSE_LOCAL --multiquery=value --query "SELECT 1" 2>&1); then
    echo 'Unexpectedly accepted a value for a zero-token option'
    exit 1
fi
[[ "$error" == *"does not take any arguments"* ]]
echo 'Zero-token option value rejected'

if error=$($CLICKHOUSE_LOCAL --unknown_client_option --query "SELECT 1" 2>&1); then
    echo 'Unexpectedly accepted an unknown option'
    exit 1
fi
[[ "$error" == *"Unrecognized option"* ]]
echo 'Unknown option rejected'
