#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function run_format()
{
    local q="$1" && shift

    echo "$q"
    $CLICKHOUSE_FORMAT <<<"$q"
}
function run_format_both()
{
    local q="$1" && shift

    echo "[multi] $q"
    $CLICKHOUSE_FORMAT <<<"$q"
    echo "[oneline] $q"
    $CLICKHOUSE_FORMAT --oneline <<<"$q"
}

# NOTE: that those queries may work slow, due to stack trace obtaining
run_format 'insert into foo settings max_threads=1' 2> >(grep -m1 -o "Syntax error (query): failed at position .* (end of query):")
run_format 'insert into foo format tsv settings max_threads=1' 2> >(grep -m1 -F -o "Can't format ASTInsertQuery with data, since data will be lost.")

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
