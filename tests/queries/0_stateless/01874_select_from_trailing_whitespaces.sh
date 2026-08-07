#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

queries=(
    "select * from system.one a"
    "select * from (select * from system.one) b, system.one a"
    "select * from system.one a, (select * from system.one) b, system.one c"
    "select * from system.one a, (select * from system.one) b, system.one c, (select * from system.one) d"
    "select * from system.one union all select * from system.one"
    "select * from system.one union all (select * from system.one)"
    "select 1 union all (select 1 union distinct select 1)"
    "select * from system.one array join arr as row"
    "select 1 in 1"
    "select 1 in (select 1)"
    "select 1 in f(1)"
    "select 1 in ((select 1) as sub)"
    "with it as ( select * from numbers(1) ) select it.number, i.number from it as i"
    "SELECT x FROM ( SELECT 1 AS x UNION ALL ( SELECT 1 UNION ALL SELECT 1))"
)
for q in "${queries[@]}"; do
    echo "# $q"
    $CLICKHOUSE_FORMAT <<<"$q"
    echo "# /* oneline */ $q"
    $CLICKHOUSE_FORMAT --oneline <<<"$q"
done
