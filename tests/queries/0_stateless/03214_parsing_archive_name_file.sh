#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function try_to_read_file()
{
    file_to_read=$1
    file_argument=$2
    settings=$3

    echo $file_argument $settings
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$file_argument') $settings" 2>&1 | grep -c "Cannot stat file.*$file_to_read"
}

# if archive extension is not detected for part before '::', path is taken as is
try_to_read_file "::nonexistentfile.csv" "::nonexistentfile.csv"
try_to_read_file "nonexistent::nonexistentfile.csv" "nonexistent::nonexistentfile.csv"
try_to_read_file "nonexistent :: nonexistentfile.csv" "nonexistent :: nonexistentfile.csv"
try_to_read_file "nonexistent ::nonexistentfile.csv" "nonexistent ::nonexistentfile.csv"
# if archive extension is detected for part before '::', path is split into archive and filename
try_to_read_file "nonexistent.tar.gz" "nonexistent.tar.gz :: nonexistentfile.csv"
try_to_read_file "nonexistent.zip" "nonexistent.zip:: nonexistentfile.csv"
# disabling archive syntax will always parse path as is
try_to_read_file "nonexistent.tar.gz :: nonexistentfile.csv" "nonexistent.tar.gz :: nonexistentfile.csv" "SETTINGS allow_archive_path_syntax=0"
try_to_read_file "nonexistent.zip:: nonexistentfile.csv" "nonexistent.zip:: nonexistentfile.csv" "SETTINGS allow_archive_path_syntax=0"
