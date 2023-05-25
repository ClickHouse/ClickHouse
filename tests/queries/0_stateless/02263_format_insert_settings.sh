#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function run_format()
{
    local q="$1" && shift

    echo "$q"
    $CLICKHOUSE_FORMAT "$@" <<<"$q"
}
function run_format_both()
{
    local q="$1" && shift

    echo "[multi] $q"
    $CLICKHOUSE_FORMAT "$@" <<<"$q"
    echo "[oneline] $q"
    $CLICKHOUSE_FORMAT --oneline "$@" <<<"$q"
}

# NOTE: that those queries may work slow, due to stack trace obtaining
run_format 'insert into foo settings max_threads=1' 2> >(grep -m1 -o "Syntax error (query): failed at position .* (end of query):")
# compatibility
run_format 'insert into foo format tsv settings max_threads=1' 2> >(grep -m1 -F -o "Can't format ASTInsertQuery with data, since data will be lost.")
run_format_both 'insert into foo format tsv settings max_threads=1' --allow_settings_after_format_in_insert
run_format 'insert into foo settings max_threads=1 format tsv settings max_threads=1' --allow_settings_after_format_in_insert 2> >(grep -m1 -F -o "You have SETTINGS before and after FORMAT")
# and via server (since this is a separate code path)
$CLICKHOUSE_CLIENT -q 'drop table if exists data_02263'
$CLICKHOUSE_CLIENT -q 'create table data_02263 (key Int) engine=Memory()'
$CLICKHOUSE_CLIENT -q 'insert into data_02263 format TSV settings max_threads=1 1' 2> >(grep -m1 -F -o "Cannot parse input: expected '\n' before: 'settings max_threads=1 1'")
$CLICKHOUSE_CLIENT --allow_settings_after_format_in_insert=1 -q 'insert into data_02263 format TSV settings max_threads=1 1'
$CLICKHOUSE_CLIENT -q 'select * from data_02263'
$CLICKHOUSE_CLIENT --allow_settings_after_format_in_insert=1 -q 'insert into data_02263 settings max_threads=1 format tsv settings max_threads=1' 2> >(grep -m1 -F -o "You have SETTINGS before and after FORMAT")
$CLICKHOUSE_CLIENT -q 'drop table data_02263'

run_format_both 'insert into foo values'
run_format_both 'insert into foo select 1'
run_format_both 'insert into foo watch bar'
run_format_both 'insert into foo format tsv'

run_format_both 'insert into foo settings max_threads=1 values'
run_format_both 'insert into foo settings max_threads=1 select 1'
run_format_both 'insert into foo settings max_threads=1 watch bar'
run_format_both 'insert into foo settings max_threads=1 format tsv'
run_format_both 'insert into foo select 1 settings max_threads=1'
run_format_both 'insert into foo settings max_threads=1 select 1 settings max_threads=1'
