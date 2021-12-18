#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists t_02151"
$CLICKHOUSE_CLIENT -q "create table t_02151 (Path String) Engine=Null"
$CLICKHOUSE_CLIENT -q "select '1' from numbers(10000000) format TSV" > t_02151.tsv

function test()
{
    for _ in {1..1000}; do
        ${CLICKHOUSE_CURL} -sS -F 's=@t_02151.tsv;' "${CLICKHOUSE_URL}&cancel_http_readonly_queries_on_client_close=1&readonly=1&s_structure=Path+String&query=SELECT+count()+FROM+t_02151+WHERE+(Path+in+s)" -o /dev/null;
    done
}

export -f test;

timeout 90 bash -c test 2>&1 
$CLICKHOUSE_CLIENT -q "drop table t_02151"
echo 'Ok'

