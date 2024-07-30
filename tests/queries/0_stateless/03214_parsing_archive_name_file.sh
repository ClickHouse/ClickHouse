#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function try_to_read_file()
{
    file_to_read=$1
    file_argument=$2

    echo $file_argument
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file_argument')" 2>&1 | rg -c "Cannot stat file.*$file_to_read"
}

try_to_read_file "::nonexistentfile.csv" "::nonexistentfile.csv"
try_to_read_file "nonexistent::nonexistentfile.csv" "nonexistent::nonexistentfile.csv"
try_to_read_file "nonexistent :: nonexistentfile.csv" "nonexistent :: nonexistentfile.csv"
try_to_read_file "nonexistent ::nonexistentfile.csv" "nonexistent ::nonexistentfile.csv"
try_to_read_file "nonexistent.tar.gz" "nonexistent.tar.gz :: nonexistentfile.csv"
try_to_read_file "nonexistent.zip" "nonexistent.zip:: nonexistentfile.csv"
